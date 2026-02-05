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


class TestLoadResponsesToSheet:
    def test_load_responses_to_sheet_combines_multiple_worksheets(self, mocker):
        """Test that _load_responses_to_sheet iterates through all worksheets and combines data"""
        # Setup mock worksheets with different data
        worksheet1_data = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2"],
                "Local Health District:": ["LHD1", "LHD1"],
                "Data": ["data1", "data2"],
            }
        )
        worksheet2_data = pd.DataFrame(
            {
                "GlobalID": ["id3", "id4"],
                "Local Health District:": ["LHD1", "LHD1"],
                "Data": ["data3", "data4"],
            }
        )
        
        # Mock worksheet objects
        mock_worksheet1 = mocker.Mock()
        mock_worksheet1.get_as_df.return_value = worksheet1_data
        mock_worksheet1.title = "Worksheet1"
        
        mock_worksheet2 = mocker.Mock()
        mock_worksheet2.get_as_df.return_value = worksheet2_data
        mock_worksheet2.title = "Worksheet2"
        
        # Mock spreadsheet to return both worksheets
        mock_spreadsheet = mocker.Mock()
        mock_spreadsheet.worksheets.return_value = [mock_worksheet1, mock_worksheet2]
        mock_spreadsheet.worksheet.return_value = mock_worksheet1  # For writing back
        
        # Mock gsheets_client
        mock_gsheets_client = mocker.Mock()
        mock_gsheets_client.open_by_key.return_value = mock_spreadsheet
        
        mocker.patch("palletjack.utils.authorize_pygsheets", return_value=mock_gsheets_client)
        
        # Setup skid instance
        skid_instance = mocker.Mock()
        skid_instance.lhd_sheet_ids = {"LHD1": "sheet_id_123"}
        skid_instance.secrets.GOOGLE_CREDENTIALS = "mock_credentials"
        
        # Create responses with a new row
        responses = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2", "id3", "id4", "id5"],
                "Local Health District:": ["LHD1", "LHD1", "LHD1", "LHD1", "LHD1"],
                "Data": ["data1", "data2", "data3", "data4", "data5_new"],
            }
        )
        
        # Call the method
        result = main.Skid._load_responses_to_sheet(skid_instance, responses, "LHD1")
        
        # Verify worksheets() was called to get all worksheets
        mock_spreadsheet.worksheets.assert_called_once()
        
        # Verify all worksheets were read during iteration
        assert mock_worksheet1.get_as_df.call_count == 2  # Once during iteration, once to get size for appending
        assert mock_worksheet2.get_as_df.call_count == 1  # Once during iteration
        
        # Verify only the new row (id5) was added
        assert result == 1
        
        # Verify set_dataframe was called with the new data
        mock_worksheet1.set_dataframe.assert_called_once()
        call_args = mock_worksheet1.set_dataframe.call_args
        added_df = call_args[0][0]
        assert len(added_df) == 1
        assert added_df["GlobalID"].iloc[0] == "id5"

    def test_load_responses_to_sheet_handles_empty_worksheets(self, mocker):
        """Test that empty worksheets are skipped when combining data"""
        # Setup mock worksheets with one empty
        worksheet1_data = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2"],
                "Local Health District:": ["LHD1", "LHD1"],
            }
        )
        worksheet2_data = pd.DataFrame()  # Empty worksheet
        
        mock_worksheet1 = mocker.Mock()
        mock_worksheet1.get_as_df.return_value = worksheet1_data
        mock_worksheet1.title = "Worksheet1"
        
        mock_worksheet2 = mocker.Mock()
        mock_worksheet2.get_as_df.return_value = worksheet2_data
        mock_worksheet2.title = "Worksheet2"
        
        mock_spreadsheet = mocker.Mock()
        mock_spreadsheet.worksheets.return_value = [mock_worksheet1, mock_worksheet2]
        mock_spreadsheet.worksheet.return_value = mock_worksheet1
        
        mock_gsheets_client = mocker.Mock()
        mock_gsheets_client.open_by_key.return_value = mock_spreadsheet
        
        mocker.patch("palletjack.utils.authorize_pygsheets", return_value=mock_gsheets_client)
        
        skid_instance = mocker.Mock()
        skid_instance.lhd_sheet_ids = {"LHD1": "sheet_id_123"}
        skid_instance.secrets.GOOGLE_CREDENTIALS = "mock_credentials"
        
        responses = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2"],
                "Local Health District:": ["LHD1", "LHD1"],
            }
        )
        
        result = main.Skid._load_responses_to_sheet(skid_instance, responses, "LHD1")
        
        # Should have read both worksheets
        assert mock_worksheet1.get_as_df.call_count == 1
        assert mock_worksheet2.get_as_df.call_count == 1
        
        # No new data to add
        assert result == 0

    def test_load_responses_to_sheet_handles_all_empty_worksheets_without_keyerror(self, mocker):
        """Test that when all worksheets are empty, an empty DataFrame with GlobalID column is created
        and no KeyError is raised when checking for new records"""
        # Setup mock worksheets that are all empty
        worksheet1_data = pd.DataFrame()  # Empty worksheet
        worksheet2_data = pd.DataFrame()  # Empty worksheet
        
        mock_worksheet1 = mocker.Mock()
        mock_worksheet1.get_as_df.return_value = worksheet1_data
        mock_worksheet1.title = "Worksheet1"
        
        mock_worksheet2 = mocker.Mock()
        mock_worksheet2.get_as_df.return_value = worksheet2_data
        mock_worksheet2.title = "Worksheet2"
        
        mock_spreadsheet = mocker.Mock()
        mock_spreadsheet.worksheets.return_value = [mock_worksheet1, mock_worksheet2]
        mock_spreadsheet.worksheet.return_value = mock_worksheet1
        
        mock_gsheets_client = mocker.Mock()
        mock_gsheets_client.open_by_key.return_value = mock_spreadsheet
        
        mocker.patch("palletjack.utils.authorize_pygsheets", return_value=mock_gsheets_client)
        
        skid_instance = mocker.Mock()
        skid_instance.lhd_sheet_ids = {"LHD1": "sheet_id_123"}
        skid_instance.secrets.GOOGLE_CREDENTIALS = "mock_credentials"
        
        # Create responses with new data
        responses = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2", "id3"],
                "Local Health District:": ["LHD1", "LHD1", "LHD1"],
                "Data": ["data1", "data2", "data3"],
            }
        )
        
        # Call the method - this should not raise a KeyError
        result = main.Skid._load_responses_to_sheet(skid_instance, responses, "LHD1")
        
        # Should have read all worksheets during iteration, plus first worksheet again to get size
        assert mock_worksheet1.get_as_df.call_count == 2  # Once during iteration, once to get size for appending
        assert mock_worksheet2.get_as_df.call_count == 1  # Once during iteration
        
        # All responses should be added since live_dataframe is empty
        assert result == 3
        
        # Verify set_dataframe was called with all the new data
        mock_worksheet1.set_dataframe.assert_called_once()
        call_args = mock_worksheet1.set_dataframe.call_args
        added_df = call_args[0][0]
        assert len(added_df) == 3
        assert set(added_df["GlobalID"].tolist()) == {"id1", "id2", "id3"}

    def test_load_responses_appends_to_empty_first_worksheet_at_row_2(self, mocker):
        """Test that new rows are appended at row 2 (header row + 1) when first worksheet is empty"""
        # Setup mock worksheets - first is empty, second has data
        worksheet1_data = pd.DataFrame()  # Empty first worksheet
        worksheet2_data = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2"],
                "Local Health District:": ["LHD1", "LHD1"],
            }
        )
        
        mock_worksheet1 = mocker.Mock()
        mock_worksheet1.get_as_df.return_value = worksheet1_data
        mock_worksheet1.title = "Worksheet1"
        
        mock_worksheet2 = mocker.Mock()
        mock_worksheet2.get_as_df.return_value = worksheet2_data
        mock_worksheet2.title = "Worksheet2"
        
        mock_spreadsheet = mocker.Mock()
        mock_spreadsheet.worksheets.return_value = [mock_worksheet1, mock_worksheet2]
        mock_spreadsheet.worksheet.return_value = mock_worksheet1
        
        mock_gsheets_client = mocker.Mock()
        mock_gsheets_client.open_by_key.return_value = mock_spreadsheet
        
        mocker.patch("palletjack.utils.authorize_pygsheets", return_value=mock_gsheets_client)
        
        skid_instance = mocker.Mock()
        skid_instance.lhd_sheet_ids = {"LHD1": "sheet_id_123"}
        skid_instance.secrets.GOOGLE_CREDENTIALS = "mock_credentials"
        
        # Create responses with new data
        responses = pd.DataFrame(
            {
                "GlobalID": ["id3", "id4"],
                "Local Health District:": ["LHD1", "LHD1"],
            }
        )
        
        result = main.Skid._load_responses_to_sheet(skid_instance, responses, "LHD1")
        
        # Should add 2 new rows
        assert result == 2
        
        # Verify set_dataframe was called with correct row index
        # Empty worksheet has 0 rows, so new data should start at row 2 (0 + 2)
        mock_worksheet1.set_dataframe.assert_called_once()
        call_args = mock_worksheet1.set_dataframe.call_args
        row_index = call_args[0][1][0]  # Get the row position (first element of the tuple)
        assert row_index == 2, f"Expected row index 2, got {row_index}"

    def test_load_responses_appends_after_existing_rows_in_first_worksheet(self, mocker):
        """Test that new rows are appended after existing rows in the first worksheet"""
        # Setup mock worksheets - first has 3 rows, second has different data
        worksheet1_data = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2", "id3"],
                "Local Health District:": ["LHD1", "LHD1", "LHD1"],
            }
        )
        worksheet2_data = pd.DataFrame(
            {
                "GlobalID": ["id4", "id5"],
                "Local Health District:": ["LHD1", "LHD1"],
            }
        )
        
        mock_worksheet1 = mocker.Mock()
        mock_worksheet1.get_as_df.return_value = worksheet1_data
        mock_worksheet1.title = "Worksheet1"
        
        mock_worksheet2 = mocker.Mock()
        mock_worksheet2.get_as_df.return_value = worksheet2_data
        mock_worksheet2.title = "Worksheet2"
        
        mock_spreadsheet = mocker.Mock()
        mock_spreadsheet.worksheets.return_value = [mock_worksheet1, mock_worksheet2]
        mock_spreadsheet.worksheet.return_value = mock_worksheet1
        
        mock_gsheets_client = mocker.Mock()
        mock_gsheets_client.open_by_key.return_value = mock_spreadsheet
        
        mocker.patch("palletjack.utils.authorize_pygsheets", return_value=mock_gsheets_client)
        
        skid_instance = mocker.Mock()
        skid_instance.lhd_sheet_ids = {"LHD1": "sheet_id_123"}
        skid_instance.secrets.GOOGLE_CREDENTIALS = "mock_credentials"
        
        # Create responses with new data not in either worksheet
        responses = pd.DataFrame(
            {
                "GlobalID": ["id1", "id2", "id3", "id4", "id5", "id6", "id7"],
                "Local Health District:": ["LHD1", "LHD1", "LHD1", "LHD1", "LHD1", "LHD1", "LHD1"],
            }
        )
        
        result = main.Skid._load_responses_to_sheet(skid_instance, responses, "LHD1")
        
        # Should add 2 new rows (id6 and id7 are not in any worksheet)
        assert result == 2
        
        # Verify set_dataframe was called with correct row index
        # First worksheet has 3 rows, so new data should start at row 5 (3 + 2)
        mock_worksheet1.set_dataframe.assert_called_once()
        call_args = mock_worksheet1.set_dataframe.call_args
        row_index = call_args[0][1][0]  # Get the row position (first element of the tuple)
        assert row_index == 5, f"Expected row index 5, got {row_index}"

