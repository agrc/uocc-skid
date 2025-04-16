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
