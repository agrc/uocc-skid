#!/usr/bin/env python
# * coding: utf8 *
"""
Run the uocc-skid as a Cloud Run instance. Uses the entry point defined in setup.py and the Dockerfile.
"""

import json
import logging
import re
import shutil
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import google.auth
import pandas as pd
from palletjack import extract, utils
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, version
except ImportError:
    import config
    import version


class Skid:
    def __init__(self):
        self.secrets = SimpleNamespace(**self._get_secrets())
        self.tempdir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.tempdir_path = Path(self.tempdir.name)
        self.log_name = f"{config.LOG_FILE_NAME}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
        self.log_path = self.tempdir_path / self.log_name
        self._initialize_supervisor()
        self.skid_logger = logging.getLogger(config.SKID_NAME)

    def __del__(self):
        self.tempdir.cleanup()

    @staticmethod
    def _get_secrets():
        """A helper method for loading secrets from either a GCF mount point or the local src/skid/secrets/secrets.json file

        Raises:
            RuntimeError: If the secrets file can't be found.

        Returns:
            dict: The secrets .json loaded as a dictionary
        """

        #: Try to get the secrets from the Cloud Function mount point
        secret_folder = Path("/secrets")
        if secret_folder.exists():
            secrets_dict = json.loads(Path("/secrets/app/secrets.json").read_text(encoding="utf-8"))

        #: Otherwise, try to load a local copy for local development
        else:
            secret_folder = Path(__file__).parent / "secrets"
            try:
                secrets_dict = json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))
            except Exception as e:
                raise RuntimeError("Secrets folder not found; secrets not loaded.") from e

        #: Authenticate with Google via ADC
        credentials, _ = google.auth.default()
        secrets_dict["GOOGLE_CREDENTIALS"] = credentials
        return secrets_dict

    def _initialize_supervisor(self):
        """A helper method to set up logging and supervisor

        Returns:
            Supervisor: The supervisor object used for sending messages
        """

        skid_logger = logging.getLogger(config.SKID_NAME)
        skid_logger.setLevel(config.LOG_LEVEL)
        palletjack_logger = logging.getLogger("palletjack")
        palletjack_logger.setLevel(config.LOG_LEVEL)

        cli_handler = logging.StreamHandler(sys.stdout)
        cli_handler.setLevel(config.LOG_LEVEL)
        formatter = logging.Formatter(
            fmt="%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        cli_handler.setFormatter(formatter)

        log_handler = logging.FileHandler(self.log_path, mode="w")
        log_handler.setLevel(config.LOG_LEVEL)
        log_handler.setFormatter(formatter)

        skid_logger.addHandler(cli_handler)
        skid_logger.addHandler(log_handler)
        palletjack_logger.addHandler(cli_handler)
        palletjack_logger.addHandler(log_handler)

        #: Log any warnings at logging.WARNING
        #: Put after everything else to prevent creating a duplicate, default formatter
        #: (all log messages were duplicated if put at beginning)
        logging.captureWarnings(True)

        skid_logger.debug("Creating Supervisor object")
        self.supervisor = Supervisor(handle_errors=False)
        sendgrid_settings = config.SENDGRID_SETTINGS
        sendgrid_settings["api_key"] = self.secrets.SENDGRID_API_KEY
        self.supervisor.add_message_handler(
            SendGridHandler(
                sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
            )
        )

    def _remove_log_file_handlers(log_name, loggers):
        """A helper function to remove the file handlers so the tempdir will close correctly

        Args:
            log_name (str): The logfiles filename
            loggers (List<str>): The loggers that are writing to log_name
        """

        for logger in loggers:
            for handler in logger.handlers:
                try:
                    if log_name in handler.stream.name:
                        logger.removeHandler(handler)
                        handler.close()
                except Exception:
                    pass

    def process(self):
        """The main function that does all the work."""

        start = datetime.now()

        #: Get our GIS object via the ArcGIS API for Python
        self.gis = arcgis.gis.GIS(config.AGOL_ORG, self.secrets.AGOL_USER, self.secrets.AGOL_PASSWORD)

        responses = self._extract_responses_from_agol()

        self.lhd_sheet_ids = {
            "BRHD": self.secrets.BRHD_SHEET_ID,
            "CUHD": self.secrets.CUHD_SHEET_ID,
            "DCHD": self.secrets.DCHD_SHEET_ID,
            "SLCoHD": self.secrets.SLCOHD_SHEET_ID,
            "SJHD": self.secrets.SJHD_SHEET_ID,
            "SEUHD": self.secrets.SEUHD_SHEET_ID,
            "SWUHD": self.secrets.SWUHD_SHEET_ID,
            "SCHD": self.secrets.SCHD_SHEET_ID,
            "TCoHD": self.secrets.TCOHD_SHEET_ID,
            "TCHD": self.secrets.TCHD_SHEET_ID,
            "UCHD": self.secrets.UCHD_SHEET_ID,
            "WCHD": self.secrets.WCHD_SHEET_ID,
            "WMHD": self.secrets.WMHD_SHEET_ID,
        }

        lhd_load_counts = []
        for lhd_abbreviation in self.lhd_sheet_ids:
            load_count = self._load_responses_to_sheet(responses, lhd_abbreviation)
            lhd_load_counts.append(f"{lhd_abbreviation}: {load_count}")

        updated_contacts_df, contact_update_status = self.update_contacts_from_responses(responses)

        locations_df = self._extract_locations_from_sheet()
        locations_df = self._add_lhd_by_county(locations_df)
        locations_df = self._clean_field_names(locations_df)
        locations_df = self._fix_apostrophes_bug(locations_df)
        locations_df.sort_values(by=["FacilityName"], inplace=True)  #: Makes name list alphabetical in survey
        locations_without_ids = locations_df[locations_df["ID"].isna()]

        #: Can reuse the updated contacts instead of getting it from the sheet again
        contacts_df = self._clean_field_names(updated_contacts_df)
        contacts_df = self._fix_apostrophes_bug(contacts_df)
        contacts_without_ids = contacts_df[contacts_df["ID"].isna()]

        update_success = self._update_items_in_survey_media_folder(locations_df, contacts_df)

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = f"{config.SKID_NAME} Update Summary"
        summary_rows = [
            f"{config.SKID_NAME} update {start.strftime('%Y-%m-%d')}",
            "=" * 20,
            "",
            f"Start time: {start.strftime('%H:%M:%S')}",
            f"End time: {end.strftime('%H:%M:%S')}",
            f"Duration: {str(end - start)}",
            f"Locations extracted: {len(locations_df)}",
            f"Locations without IDs: {len(locations_without_ids)}",
            f"Contacts extracted: {len(contacts_df)}",
            f"Contacts without IDs: {len(contacts_without_ids)}",
            "",
        ]
        if update_success:
            summary_rows.append("Survey media folder updated successfully")
        else:
            summary_rows.append("Survey media folder update failed")

        summary_rows.append("")
        summary_rows.append("Responses loaded to sheets:")
        summary_rows.extend(lhd_load_counts)
        summary_rows.append("")
        summary_rows.append(contact_update_status)

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = self.tempdir_path / self.log_name

        self.supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        self._remove_log_file_handlers(loggers)

    def _extract_locations_from_sheet(self):
        gsheet_extractor = extract.GSheetLoader(self.secrets.GOOGLE_CREDENTIALS)
        uocc_df = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self.secrets.UOCC_LOCATIONS_SHEET_ID, "UOCCs", by_title=True
        )
        uocc_df["ID#"] = uocc_df["ID#"].astype(str)

        uocc_df.drop(
            columns=[
                "Local Health Department",
                "UOCC Email Address",
                "Corporate Email Address",
                "Corporate Contact Name",
                "UOCC Contact Name",
            ],
            inplace=True,
        )

        return uocc_df[uocc_df["Status"].str.lower() == "open"].copy()

    def _add_lhd_by_county(self, locations_df):
        counties_to_lhd = {
            "Box Elder": "BRHD",
            "Cache": "BRHD",
            "Rich": "BRHD",
            "Weber": "WMHD",
            "Morgan": "WMHD",
            "Davis": "DCHD",
            "Salt Lake": "SLCoHD",
            "Utah": "UCHD",
            "Wasatch": "WCHD",
            "Summit": "SCHD",
            "Juab": "CUHD",
            "Millard": "CUHD",
            "Piute": "CUHD",
            "Sanpete": "CUHD",
            "Sevier": "CUHD",
            "Wayne": "CUHD",
            "Tooele": "TCoHD",
            "Beaver": "SWUHD",
            "Iron": "SWUHD",
            "Kane": "SWUHD",
            "Washington": "SWUHD",
            "Garfield": "SWUHD",
            "San Juan": "SJHD",
            "Grand": "SEUHD",
            "Emery": "SEUHD",
            "Carbon": "SEUHD",
            "Duchesne": "TCHD",
            "Daggett": "TCHD",
            "Uintah": "TCHD",
        }

        locations_df["lhd"] = locations_df["County"].apply(lambda x: str(x).strip()).replace(counties_to_lhd)

        return locations_df

    def _clean_field_names(self, df):
        #: Can't have newlines, spaces, or hashes in the column names for the S123 app to work
        df.columns = [col.replace("\n", "").replace(" ", "").replace("#", "") for col in df.columns]

        return df

    def _fix_apostrophes_bug(self, df):
        #: re https://support.esri.com/en-us/bug/an-error-occurs-when-trying-to-edit-an-arcgis-survey123-bug-000146547
        #: but the HTML entity code isn't working either, so just remove the apostrophes

        df["FacilityName"] = df["FacilityName"].str.replace("'", "")
        return df

    def _extract_contacts_from_sheet(self):
        gsheet_extractor = extract.GSheetLoader(self.secrets.GOOGLE_CREDENTIALS)
        contacts_df = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self.secrets.UOCC_CONTACTS_SHEET_ID, "UOCC Contacts", by_title=True
        )

        contacts_df["ID#"] = contacts_df["ID#"].astype(str)

        return contacts_df

    def _update_items_in_survey_media_folder(self, locations_df, contacts_df):
        #: Get the survey properties so we can use the title for the zipfile name
        survey_manager = arcgis.apps.survey123.SurveyManager(self.gis)
        survey_properties = survey_manager.get(self.secrets.SURVEY_ITEMID).properties

        #: Download and extract the survey form
        survey_item = self.gis.content.get(self.secrets.SURVEY_ITEMID)
        downloaded_zip = survey_item.download(save_path=self.tempdir.name)

        with zipfile.ZipFile(downloaded_zip) as zipped_file:
            zipped_file.extractall(self.tempdir_path / "_survey")

        #: Overwrite the locations and contacts with the new data
        locations_df.to_csv(self.tempdir_path / "_survey/esriinfo/media/locations_with_lhd.csv", index=False)
        contacts_df.to_csv(self.tempdir_path / "_survey/esriinfo/media/uocc_contacts.csv", index=False)

        #: Remove old zip file to avoid conflicts
        Path(downloaded_zip).unlink()

        #: Re-zip the survey form with the new data, update the AGOL item
        new_zip_name = survey_properties["title"]
        new_zip = shutil.make_archive(self.tempdir_path / new_zip_name, "zip", self.tempdir_path / "_survey")

        update_success = survey_item.update({}, new_zip)

        return update_success

    def _extract_responses_from_agol(self):
        #: Download from AGOL
        feature_layer = self.gis.content.get(self.secrets.RESULTS_ITEMID).layers[0]
        responses = feature_layer.query(return_geometry=False, as_df=True)

        #: Drop unneeded columns
        columns_to_drop = [
            "objectid",
            "password_entry",
            "logo",
            "photos_please_upload",
            "CreationDate",
            "Creator",
            "EditDate",
            "Editor",
        ]
        responses.drop(columns=columns_to_drop, errors="ignore", inplace=True)

        #: Rename columns to match the aliases in the survey, including numbering
        alias_mapper = {field["name"]: field["alias"] for field in feature_layer.properties.fields}
        new_column_names = self._map_aliases_to_columns(alias_mapper)

        return responses.rename(columns=new_column_names)

    @staticmethod
    def _map_aliases_to_columns(alias_mapper_dict):
        number_regex = re.compile(r"\d{1,2}\. ")
        sub_number_regex = re.compile(r"\d{1,2}[a-z]{1}\. ")

        current = ""
        end_field = "assistance"
        for field, alias in alias_mapper_dict.items():
            if field == end_field:
                break
            if number_regex.search(alias):
                current = number_regex.search(alias).group(0)
                continue
            if sub_number_regex.search(alias):
                continue
            if current:
                new_alias = current + alias
                alias_mapper_dict[field] = new_alias
                continue

        return alias_mapper_dict

    def _load_responses_to_sheet(self, responses, lhd_abbreviation):
        """Load the responses to the Google Sheet

        Args:
            responses (pd.DataFrame): The DataFrame containing the responses to load
            lhd_abbreviation (str): The abbreviation for the local health department
        """

        self.skid_logger.debug(
            "Loading responses to the %s sheet with id %s", lhd_abbreviation, self.lhd_sheet_ids[lhd_abbreviation]
        )
        gsheets_client = utils.authorize_pygsheets(self.secrets.GOOGLE_CREDENTIALS)
        lhd_worksheet = gsheets_client.open_by_key(self.lhd_sheet_ids[lhd_abbreviation]).worksheet("index", 0)
        live_dataframe = lhd_worksheet.get_as_df()
        lhd_dataframe = responses[responses["Local Health District:"] == lhd_abbreviation].copy()
        adds = lhd_dataframe[~lhd_dataframe["GlobalID"].isin(live_dataframe["GlobalID"])]
        if not adds.empty:
            lhd_worksheet.set_dataframe(
                adds, (live_dataframe.shape[0] + 2, 1), copy_index=False, copy_head=False, extend=True, nan=""
            )

            add_count = adds.shape[0]
            self.skid_logger.debug("Added %s new responses to the %s sheet", add_count, lhd_abbreviation)
            return add_count

        self.skid_logger.debug("No new responses to add to the %s sheet", lhd_abbreviation)
        return 0

    def update_contacts_from_responses(self, responses: pd.DataFrame) -> tuple[pd.DataFrame, str]:
        """Master method for the contact update process"""

        existing_contacts_df = self._extract_contacts_from_sheet()
        new_contacts_df = self._extract_contact_updates_from_responses(responses)
        if new_contacts_df.empty:
            contact_update_status = "No contact updates found in responses"
            return existing_contacts_df, contact_update_status

        new_contacts_df = self._clean_contacts_dataframe(new_contacts_df)
        updated_contacts_df = self._update_existing_contacts_dataframe(existing_contacts_df, new_contacts_df)
        try:
            self._load_updates_to_contacts_sheet(updated_contacts_df)
            contact_update_status = "Contacts sheet updated successfully"
        except Exception as e:
            self.skid_logger.error("Error updating contacts sheet: %s", e)
            contact_update_status = f"Contacts sheet update failed: {e}"
        return updated_contacts_df, contact_update_status

    def _extract_contact_updates_from_responses(self, responses: pd.DataFrame) -> pd.DataFrame:
        """Extract the contact updates from the responses DataFrame, only taking the latest update per ID

        Args:
            responses (pd.DataFrame): Responses loaded from AGOL

        Returns:
            pd.DataFrame: Responses filtered to rows with response flag set and only name/email columns
        """

        new_contact_info = responses[responses["Is this information still correct?"] == "no"][
            ["UOCC Facility Code:", "UOCC Manager or Contact Name:", "UOCC Email Address:", "date_of_signature"]
        ].copy()
        new_contact_info.sort_values(by=["date_of_signature"], ascending=False, inplace=True)
        new_contact_info.drop_duplicates(subset=["UOCC Facility Code:"], keep="first", inplace=True)
        new_contact_info.drop(columns=["date_of_signature"], inplace=True)

        return new_contact_info

    def _clean_contacts_dataframe(self, new_contacts: pd.DataFrame) -> pd.DataFrame:
        """Align new contacts dataframe for a df.update

        Args:
            new_contacts (pd.DataFrame): Contacts to update from responses

        Returns:
            pd.DataFrame: Columns renamed, types set, index set
        """

        survey_to_live_mapping = {
            "UOCC Facility Code:": "ID#",
            "UOCC Manager or Contact Name:": "UOCC Contact Name",
            "UOCC Email Address:": "UOCC Email Address",
        }
        new_contacts.rename(columns=survey_to_live_mapping, inplace=True)
        for column in new_contacts.columns:
            new_contacts[column] = new_contacts[column].astype("object")

        new_contacts.set_index("ID#", inplace=True)

        return new_contacts

    def _update_existing_contacts_dataframe(
        self, live_contacts: pd.DataFrame, new_contacts: pd.DataFrame
    ) -> pd.DataFrame:
        """Update the live contacts dataframe with the new contacts dataframe

        Args:
            live_contacts (pd.DataFrame): The existing contacts dataframe from the contacts sheet
            new_contacts (pd.DataFrame): The new contacts dataframe from the responses

        Returns:
            pd.DataFrame: The updated contacts dataframe
        """

        #: get so we can return the ID column to the right place after the reindex
        id_column_index = live_contacts.columns.get_loc("ID#")

        live_contacts.set_index("ID#", inplace=True)
        live_contacts.update(new_contacts)

        live_contacts.reset_index(inplace=True)

        #: Reshuffle ID column back to original location
        cols = live_contacts.columns.tolist()
        cols.insert(id_column_index, cols.pop(cols.index("ID#")))
        live_contacts = live_contacts.reindex(columns=cols)

        return live_contacts

    def _load_updates_to_contacts_sheet(self, updated_contacts: pd.DataFrame):
        """Load the updated contacts dataframe to the contacts sheet

        Args:
            updated_contacts (pd.DataFrame): The updated contacts dataframe
        """

        self.skid_logger.debug(
            "Loading updated contacts to the contacts sheet with id %s", self.secrets.UOCC_CONTACTS_SHEET_ID
        )
        gsheets_client = utils.authorize_pygsheets(self.secrets.GOOGLE_CREDENTIALS)
        contacts_worksheet = gsheets_client.open_by_key(self.secrets.UOCC_CONTACTS_SHEET_ID).worksheet(
            "title", "UOCC Contacts"
        )
        contacts_worksheet.set_dataframe(
            updated_contacts,
            (1, 1),
            copy_index=False,
            copy_head=True,
            extend=True,
            nan="",
        )


def entry():
    uocc_skid = Skid()
    uocc_skid.process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    uocc_skid = Skid()
    uocc_skid.process()
