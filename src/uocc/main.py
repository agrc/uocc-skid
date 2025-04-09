#!/usr/bin/env python
# * coding: utf8 *
"""
Run the SKIDNAME script as a cloud function.
"""

import base64
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import functions_framework
import google.auth
import pandas as pd
from cloudevents.http import CloudEvent
from palletjack import extract, load, transform
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
            FileNotFoundError: If the secrets file can't be found.

        Returns:
            dict: The secrets .json loaded as a dictionary
        """

        secret_folder = Path("/secrets")

        #: Try to get the secrets from the Cloud Function mount point
        if secret_folder.exists():
            secrets_dict = json.loads(Path("/secrets/app/secrets.json").read_text(encoding="utf-8"))
            credentials, _ = google.auth.default()
            secrets_dict["SERVICE_ACCOUNT_JSON"] = credentials
            return secrets_dict

        #: Otherwise, try to load a local copy for local development
        secret_folder = Path(__file__).parent / "secrets"
        if secret_folder.exists():
            return json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))

        raise FileNotFoundError("Secrets folder not found; secrets not loaded.")

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
        gis = arcgis.gis.GIS(config.AGOL_ORG, self.secrets.AGOL_USER, self.secrets.AGOL_PASSWORD)

        locations_df = self._extract_locations_from_sheet()
        locations_df = self._add_lhd_by_county(locations_df)
        contacts_df = self._extract_contacts_from_sheet()

        locations_count = self._load_locations_to_agol(gis, locations_df)
        contacts_count = self._load_contacts_to_agol(gis, contacts_df)

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
            f"Locations loaded: {locations_count}",
            f"Contacts loaded: {contacts_count}",
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = self.tempdir_path / self.log_name

        self.supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        self._remove_log_file_handlers(loggers)

    def _extract_locations_from_sheet(self):
        gsheet_extractor = extract.GSheetLoader(self.secrets.SERVICE_ACCOUNT_JSON)
        uocc_df = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self.secrets.UOCC_LOCATIONS_SHEET_ID, "UOCCs", by_title=True
        )

        renamed_df = (
            transform.DataCleaning.rename_dataframe_columns_for_agol(uocc_df)
            .rename(
                columns={
                    "Longitude_": "Longitude",
                    "Accept_Material__Dropped___Off_by_the_Public": "Accept_Material_Dropped_Off_by_",
                    "Gallons_of_Used_Oil_Collected_for_Recycling_Last_Year": "Gallons_of_Used_Oil_Collected_f",
                }
            )
            .drop(
                columns=[
                    "Local_Health_Department",
                    "UOCC_Email_Address",
                    "Corporate_Email_Address",
                    "Corporate_Contact_Name",
                    "UOCC_Contact_Name",
                ]
            )
        )
        renamed_df["ID_"] = renamed_df["ID_"].astype(str)

        return renamed_df

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
            "Tooele": "TCHD",
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

    def _load_locations_to_agol(self, gis, locations_df):
        self.skid_logger.info("Creating, projecting, and cleaning spatial location data...")
        #: Drop empty lat/long
        locations_df = locations_df[locations_df["Latitude"].astype(bool) & locations_df["Longitude"].astype(bool)]
        spatial_df = pd.DataFrame.spatial.from_xy(locations_df, "Longitude", "Latitude")
        spatial_df.spatial.project(3857)
        spatial_df.spatial.set_geometry("SHAPE")
        spatial_df.spatial.sr = {"wkid": 3857}
        spatial_df = transform.DataCleaning.switch_to_float(
            spatial_df,
            [
                "Latitude",
                "Longitude",
                "Gallons_of_Used_Oil_Collected_f",
            ],
        )
        spatial_df = transform.DataCleaning.switch_to_nullable_int(spatial_df, ["Zip_Code"])

        self.skid_logger.info("Truncating and loading location data...")
        updater = load.ServiceUpdater(gis, self.secrets.UOCC_LOCATIONS_ITEMID, working_dir=self.tempdir_path)
        load_count = updater.truncate_and_load(spatial_df)
        return load_count

    def _extract_contacts_from_sheet(self):
        gsheet_extractor = extract.GSheetLoader(self.secrets.SERVICE_ACCOUNT_JSON)
        contacts_df = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self.secrets.UOCC_CONTACTS_SHEET_ID, "UOCC Contacts", by_title=True
        )

        renamed_df = transform.DataCleaning.rename_dataframe_columns_for_agol(contacts_df)
        renamed_df["ID_"] = renamed_df["ID_"].astype(str)

        return renamed_df

    def _load_contacts_to_agol(self, gis, contacts_df):
        self.skid_logger.info("Truncating and loading contact data...")
        updater = load.ServiceUpdater(
            gis, self.secrets.UOCC_CONTACTS_ITEMID, service_type="table", working_dir=self.tempdir_path
        )
        load_count = updater.truncate_and_load(contacts_df)
        return load_count


@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """Entry point for Google Cloud Function triggered by pub/sub event

    Args:
         cloud_event (CloudEvent):  The CloudEvent object with data specific to this type of
                        event. The `type` field maps to
                         `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                        The `data` field maps to the PubsubMessage data
                        in a base64-encoded string. The `attributes` field maps
                        to the PubsubMessage attributes if any is present.
    Returns:
        None. The output is written to Cloud Logging.
    """

    #: This function must be called 'subscribe' to act as the Google Cloud Function entry point. It must accept the
    #: CloudEvent object as the only argument.

    #: You can get the message-body value from the Cloud Scheduler event sent via pub/sub to customize the run
    if base64.b64decode(cloud_event.data["message"]["data"]).decode() == "foo":
        pass

    #: Call process() and any other functions you want to be run as part of the skid here.
    uocc_skid = Skid()
    uocc_skid.process()


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    uocc_skid = Skid()
    uocc_skid.process()
