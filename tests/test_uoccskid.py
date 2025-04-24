import pandas as pd

from uocc import main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}


def test_get_secrets_from_local_location(mocker):
    exists_mock = mocker.Mock(side_effect=[False, True])
    mocker.patch("pathlib.Path.exists", new=exists_mock)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}
    assert exists_mock.call_count == 2


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
            "certify": "This is certified",
            "qux": "Shouldn't have number",
        }

        expected_mapping = {
            "foo": "1. First Question",
            "bar": "1a. First Question sub question",
            "baz": "2. Second Question",
            "certify": "This is certified",
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
            "certify": "This is certified",
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
            "certify": "This is certified",
            "qux": "Shouldn't have number",
        }

        new_mapping = main.Skid._map_aliases_to_columns(input_mapping)

        assert new_mapping == expected_mapping
