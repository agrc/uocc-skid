import pandas as pd
import pytest

from uocc import main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')
    mocker.patch("google.auth.default", return_value=("sa", 42))

    secrets = main.Skid._get_secrets()

    assert secrets == {"foo": "bar", "GOOGLE_CREDENTIALS": "sa"}


def test_get_secrets_from_local_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=False)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')
    mocker.patch("google.auth.default", return_value=("local_adc", 42))

    secrets = main.Skid._get_secrets()

    assert secrets == {"foo": "bar", "GOOGLE_CREDENTIALS": "local_adc"}


def test_get_secrets_raises_if_no_secrets(mocker):
    mocker.patch("pathlib.Path.exists", return_value=False)
    mocker.patch("pathlib.Path.read_text", side_effect=FileNotFoundError)

    with pytest.raises(RuntimeError) as excinfo:
        main.Skid._get_secrets()
        assert "Secrets folder not found; secrets not loaded." in str(excinfo.value)


class TestLocationsExtracting:
    def test_extract_locations_from_sheet_only_returns_opens(self, mocker):
        input_dataframe = pd.DataFrame(
            {
                "Status": ["Deactivated", "open", "Open"],
                "ID#": ["UOCC-1234", "UOCC-5678", "UOCC-9101"],
                "Local Health Department": ["foo", "bar", "baz"],
                "UOCC Email Address": ["foo", "bar", "baz"],
                "Corporate Email Address": ["foo", "bar", "baz"],
                "Corporate Contact Name": ["foo", "bar", "baz"],
                "UOCC Contact Name": ["foo", "bar", "baz"],
            }
        )

        GSheetLoader_mock = mocker.patch("uocc.main.extract.GSheetLoader")
        GSheetLoader_mock.return_value.load_specific_worksheet_into_dataframe.return_value = input_dataframe

        output_dataframe = main.Skid._extract_locations_from_sheet(mocker.Mock())

        expected_dataframe = pd.DataFrame(
            {
                "Status": ["open", "Open"],
                "ID#": ["UOCC-5678", "UOCC-9101"],
            },
            index=[1, 2],
        )

        pd.testing.assert_frame_equal(expected_dataframe, output_dataframe)

    def test_clean_location_field_names_removes_newlines_spaces_hashes(self, mocker):
        input_dataframe = pd.DataFrame(
            {
                "Status": ["open", "Open"],
                "Accepts Materials\nDropped Off\nBy Public": ["foo", "bar"],
                "Facility Name": ["foo", "bar"],
                "ID#": ["foo", "bar"],
            }
        )

        output_dataframe = main.Skid._clean_field_names(mocker.Mock(), input_dataframe)

        expected_dataframe = pd.DataFrame(
            {
                "Status": ["open", "Open"],
                "AcceptsMaterialsDroppedOffByPublic": ["foo", "bar"],
                "FacilityName": ["foo", "bar"],
                "ID": ["foo", "bar"],
            },
        )

        pd.testing.assert_frame_equal(expected_dataframe, output_dataframe)

    def test_fix_apostrophes_bug_replaces_with_html_entity(self, mocker):
        input_dataframe = pd.DataFrame(
            {
                "FacilityName": ["Autozone", "O'Reilly"],
            }
        )

        output_dataframe = main.Skid._fix_apostrophes_bug(mocker.Mock(), input_dataframe)

        expected_dataframe = pd.DataFrame(
            {
                "FacilityName": ["Autozone", "OReilly"],
            },
        )

        pd.testing.assert_frame_equal(expected_dataframe, output_dataframe)


class TestQuestionsRenaming:
    def test_map_aliases_to_columns_assigns_numbers_to_sub_fields(self):
        input_mapping = {
            "foo": "1. First Question",
            "bar": "Comments",
            "baz": "2. Second Question",
        }

        expected_mapping = {
            "foo": "1. First Question",
            "bar": "1. Comments",
            "baz": "2. Second Question",
        }

        new_mapping = main.Skid._map_aliases_to_columns(input_mapping)

        assert new_mapping == expected_mapping

    def test_map_aliases_to_columns_skips_sub_field_with_character_identifier(self):
        input_mapping = {
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "baz": "2. Second Question",
        }

        expected_mapping = {
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "baz": "2. Second Question",
        }

        new_mapping = main.Skid._map_aliases_to_columns(input_mapping)

        assert new_mapping == expected_mapping

    def test_map_aliases_to_columns_stops_at_end_field(self):
        input_mapping = {
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "baz": "2. Second Question",
            "assistance": "Requesting Assistance",
            "qux": "Shouldn't have number",
        }

        expected_mapping = {
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "baz": "2. Second Question",
            "assistance": "Requesting Assistance",
            "qux": "Shouldn't have number",
        }

        new_mapping = main.Skid._map_aliases_to_columns(input_mapping)

        assert new_mapping == expected_mapping

    def test_map_aliases_to_columns_doesnt_modify_pre_question_fields(self):
        input_mapping = {
            "city": "Enter City",
            "county": "Enter County",
            "foo": "1. First Question",
            "bar": "2. First Question",
            "baz": "3. Second Question",
        }

        expected_mapping = {
            "city": "Enter City",
            "county": "Enter County",
            "foo": "1. First Question",
            "bar": "2. First Question",
            "baz": "3. Second Question",
        }

        new_mapping = main.Skid._map_aliases_to_columns(input_mapping)

        assert new_mapping == expected_mapping

    def test_map_aliases_to_columns_happy_path(self):
        input_mapping = {
            "city": "Enter City",
            "county": "Enter County",
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "bar_comments": "1. Comments",
            "baz": "2. Second Question",
            "baz_comments": "2. Comments",
            "assistance": "Requesting Assistance",
            "qux": "Shouldn't have number",
        }

        expected_mapping = {
            "city": "Enter City",
            "county": "Enter County",
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "bar_comments": "1. Comments",
            "baz": "2. Second Question",
            "baz_comments": "2. Comments",
            "assistance": "Requesting Assistance",
            "qux": "Shouldn't have number",
        }

        new_mapping = main.Skid._map_aliases_to_columns(input_mapping)

        assert new_mapping == expected_mapping


class TestContactUpdating:
    def test_extract_contact_updates_from_responses_only_keeps_latest_update_per_id(self, mocker):
        responses = pd.DataFrame(
            {
                "UOCC Facility Code:": ["UOCC-1234", "UOCC-5678", "UOCC-1234"],
                "UOCC Manager or Contact Name:": ["Alice", "Bob", "Charlie"],
                "UOCC Email Address:": ["foo@bar.com", "bar@baz.com", "baz@bar.com"],
                "date_of_signature": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "Is this information still correct?": ["no", "no", "no"],
            }
        )
        responses["date_of_signature"] = responses["date_of_signature"].astype("datetime64[ns]")

        expected_output = pd.DataFrame(
            {
                "UOCC Facility Code:": ["UOCC-1234", "UOCC-5678"],
                "UOCC Manager or Contact Name:": ["Charlie", "Bob"],
                "UOCC Email Address:": ["baz@bar.com", "bar@baz.com"],
            },
            index=[2, 1],
        )

        output = main.Skid._extract_contact_updates_from_responses(mocker.Mock, responses)

        pd.testing.assert_frame_equal(expected_output, output)

    def test_extract_contact_updates_returns_empty_dataframe_if_no_updates(self, mocker):
        responses = pd.DataFrame(
            {
                "UOCC Facility Code:": ["UOCC-1234", "UOCC-5678", "UOCC-1234"],
                "UOCC Manager or Contact Name:": ["Alice", "Bob", "Charlie"],
                "UOCC Email Address:": ["foo@bar.com", "bar@baz.com", "baz@bar.com"],
                "date_of_signature": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "Is this information still correct?": ["yes", "yes", "yes"],
            }
        )
        responses["date_of_signature"] = responses["date_of_signature"].astype("datetime64[ns]")

        expected_output = pd.DataFrame(
            {
                "UOCC Facility Code:": [],
                "UOCC Manager or Contact Name:": [],
                "UOCC Email Address:": [],
            },
        )
        expected_output["UOCC Facility Code:"] = expected_output["UOCC Facility Code:"].astype("object")
        expected_output["UOCC Manager or Contact Name:"] = expected_output["UOCC Manager or Contact Name:"].astype(
            "object"
        )
        expected_output["UOCC Email Address:"] = expected_output["UOCC Email Address:"].astype("object")

        output = main.Skid._extract_contact_updates_from_responses(mocker.Mock, responses)

        pd.testing.assert_frame_equal(expected_output, output)

    def test_clean_contact_dataframe_renames_columns_and_sets_types(self, mocker):
        new_contacts = pd.DataFrame(
            {
                "UOCC Facility Code:": ["UOCC-1234", "UOCC-5678"],
                "UOCC Manager or Contact Name:": ["Charlie", "Bob"],
                "UOCC Email Address:": ["foo@bar.com", "bar@baz.com"],
            }
        )
        new_contacts["UOCC Facility Code:"] = new_contacts["UOCC Facility Code:"].astype("string")
        new_contacts["UOCC Manager or Contact Name:"] = new_contacts["UOCC Manager or Contact Name:"].astype("string")
        new_contacts["UOCC Email Address:"] = new_contacts["UOCC Email Address:"].astype("string")

        expected_output = pd.DataFrame(
            {
                "UOCC Contact Name": ["Charlie", "Bob"],
                "UOCC Email Address": ["foo@bar.com", "bar@baz.com"],
            },
            index=["UOCC-1234", "UOCC-5678"],
        )
        expected_output.index.name = "ID#"
        expected_output.index = expected_output.index.astype("object")
        expected_output["UOCC Contact Name"] = expected_output["UOCC Contact Name"].astype("object")
        expected_output["UOCC Email Address"] = expected_output["UOCC Email Address"].astype("object")

        output = main.Skid._clean_contacts_dataframe(mocker.Mock, new_contacts)
        pd.testing.assert_frame_equal(expected_output, output)

    def test_update_existing_contacts_dataframe_updates_and_handles_other_fields(self, mocker):
        existing_contacts = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2", "Loc3"],
                "ID#": ["UOCC-1234", "UOCC-5678", "UOCC-9101"],
                "Local Health Department": ["X", "Y", "Z"],
                "UOCC Contact Name": ["Alice", "Bob", "Eve"],
                "UOCC Email Address": ["boo", "bar", "baz"],
                "Corporate Contact Name": ["C1", "C2", "C3"],
                "Corporate Email Address": ["c1", "c2", "c3"],
            }
        )
        new_contacts = pd.DataFrame(
            {
                "UOCC Contact Name": ["Charlie", "Bob"],
                "UOCC Email Address": ["charlie", "bob"],
            },
            index=["UOCC-1234", "UOCC-5678"],
        )
        new_contacts.index.name = "ID#"
        new_contacts.index = new_contacts.index.astype("object")
        for col in new_contacts.columns:
            new_contacts[col] = new_contacts[col].astype("object")

        expected_output = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2", "Loc3"],
                "ID#": ["UOCC-1234", "UOCC-5678", "UOCC-9101"],
                "Local Health Department": ["X", "Y", "Z"],
                "UOCC Contact Name": ["Charlie", "Bob", "Eve"],
                "UOCC Email Address": ["charlie", "bob", "baz"],
                "Corporate Contact Name": ["C1", "C2", "C3"],
                "Corporate Email Address": ["c1", "c2", "c3"],
            }
        )

        output = main.Skid._update_existing_contacts_dataframe(mocker.Mock, existing_contacts, new_contacts)
        pd.testing.assert_frame_equal(expected_output, output)

    def test_update_existing_contacts_dataframe_does_not_add_new_contact(self, mocker):
        existing_contacts = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2"],
                "ID#": ["UOCC-1234", "UOCC-5678"],
                "Local Health Department": ["X", "Y"],
                "UOCC Contact Name": ["Alice", "Bob"],
                "UOCC Email Address": ["boo", "bar"],
                "Corporate Contact Name": ["C1", "C2"],
                "Corporate Email Address": ["c1", "c2"],
            }
        )
        new_contacts = pd.DataFrame(
            {
                "UOCC Contact Name": ["Charlie", "David"],
                "UOCC Email Address": ["charlie", "david"],
            },
            index=["UOCC-1234", "UOCC-9101"],  # UOCC-9101 does not exist in existing_contacts
        )
        new_contacts.index.name = "ID#"
        new_contacts.index = new_contacts.index.astype("object")
        for col in new_contacts.columns:
            new_contacts[col] = new_contacts[col].astype("object")

        expected_output = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2"],
                "ID#": ["UOCC-1234", "UOCC-5678"],
                "Local Health Department": ["X", "Y"],
                "UOCC Contact Name": ["Charlie", "Bob"],
                "UOCC Email Address": ["charlie", "bar"],
                "Corporate Contact Name": ["C1", "C2"],
                "Corporate Email Address": ["c1", "c2"],
            }
        )

        output = main.Skid._update_existing_contacts_dataframe(mocker.Mock, existing_contacts, new_contacts)
        pd.testing.assert_frame_equal(expected_output, output)

    def test_update_existing_contacts_dataframe_handles_missing_corporate_values(self, mocker):
        existing_contacts = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2"],
                "ID#": ["UOCC-1234", "UOCC-5678"],
                "Local Health Department": ["X", "Y"],
                "UOCC Contact Name": ["Alice", "Bob"],
                "UOCC Email Address": ["boo", "bar"],
                "Corporate Contact Name": [None, "C2"],
                "Corporate Email Address": [None, "c2"],
            }
        )
        new_contacts = pd.DataFrame(
            {
                "UOCC Contact Name": ["Charlie", "David"],
                "UOCC Email Address": ["charlie", "david"],
            },
            index=["UOCC-1234", "UOCC-5678"],
        )
        new_contacts.index.name = "ID#"
        new_contacts.index = new_contacts.index.astype("object")
        for col in new_contacts.columns:
            new_contacts[col] = new_contacts[col].astype("object")

        expected_output = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2"],
                "ID#": ["UOCC-1234", "UOCC-5678"],
                "Local Health Department": ["X", "Y"],
                "UOCC Contact Name": ["Charlie", "David"],
                "UOCC Email Address": ["charlie", "david"],
                "Corporate Contact Name": [None, "C2"],
                "Corporate Email Address": [None, "c2"],
            }
        )

        output = main.Skid._update_existing_contacts_dataframe(mocker.Mock, existing_contacts, new_contacts)
        pd.testing.assert_frame_equal(expected_output, output)

    def test_update_contacts_from_responses_no_updates_returns_original_contacts(self, mocker):
        existing_contacts = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2"],
                "ID#": ["UOCC-1234", "UOCC-5678"],
                "Local Health Department": ["X", "Y"],
                "UOCC Contact Name": ["Alice", "Bob"],
                "UOCC Email Address": ["boo", "bar"],
                "Corporate Contact Name": ["C1", "C2"],
                "Corporate Email Address": ["c1", "c2"],
            }
        )
        expected_output = existing_contacts.copy()

        skid_mock = mocker.Mock()
        skid_mock._extract_contacts_from_sheet.return_value = existing_contacts
        skid_mock._extract_contact_updates_from_responses.return_value = pd.DataFrame(
            {
                "UOCC Facility Code:": [],
                "UOCC Manager or Contact Name:": [],
                "UOCC Email Address:": [],
            },
        )

        updated_contacts, update_status = main.Skid.update_contacts_from_responses(skid_mock, mocker.Mock())

        pd.testing.assert_frame_equal(expected_output, updated_contacts)
        assert update_status == "No contact updates found in responses"
        skid_mock._clean_contacts_dataframe.assert_not_called()
        skid_mock._update_existing_contacts_dataframe.assert_not_called()
        skid_mock._load_updates_to_contacts_sheet.assert_not_called()

    def test_update_contacts_from_responses_logs_update_error_and_returns(self, mocker):
        existing_contacts = pd.DataFrame(
            {
                "Facility Name": ["Loc1", "Loc2"],
                "ID#": ["UOCC-1234", "UOCC-5678"],
                "Local Health Department": ["X", "Y"],
                "UOCC Contact Name": ["Alice", "Bob"],
                "UOCC Email Address": ["boo", "bar"],
                "Corporate Contact Name": ["C1", "C2"],
                "Corporate Email Address": ["c1", "c2"],
            }
        )

        skid_mock = mocker.Mock()
        skid_mock._extract_contact_updates_from_responses.return_value = pd.DataFrame(
            {"foo": ["bar"]}  # dummy non-empty dataframe to pass empty check
        )
        skid_mock._load_updates_to_contacts_sheet.side_effect = Exception("Loading error")
        skid_mock._update_existing_contacts_dataframe.return_value = existing_contacts.copy()

        updated_contacts, update_status = main.Skid.update_contacts_from_responses(skid_mock, mocker.Mock())

        pd.testing.assert_frame_equal(existing_contacts, updated_contacts)
        assert update_status == "Contacts sheet update failed: Loading error"

        #: Getting the message is being tricky, debug shows its got the right args
        skid_mock.skid_logger.error.assert_called_once()
