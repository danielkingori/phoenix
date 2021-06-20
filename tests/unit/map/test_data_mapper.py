import pytest
from phoenix.common import utils
from phoenix.map.data_mapper import DataMapper
from unittest.mock import patch


@pytest.fixture(scope="module")
def fb_post_mock_input_data():
    p = utils.relative_path("./fb_post_mock_input_data.json", __file__)
    with open(p) as json_file:
        return json_file.read()


@pytest.fixture(scope="module")
def fb_account_mock_output_data():
    p = utils.relative_path("./fb_account_output_data.json", __file__)
    with open(p) as json_file:
        return json_file.read()


@patch(
    "phoenix.map.config.config.get_map_config",
    autospec=True
)
def test_data_mapper_object_init(mock_get_map_config):
    mock_get_map_config.return_value = [{'key': 'value'}]
    data_mapper = DataMapper(data_origin='fb')
    assert data_mapper.data_origin == 'fb'
    assert data_mapper._map_config == [{'key': 'value'}]
    assert data_mapper.expected_output_count == 1
    assert data_mapper.files_to_process == []
    assert data_mapper.output_files == []


@patch(
    "phoenix.map.config.config.get_map_config",
    autospec=True
)
@patch(
    "phoenix.map.config.config.get_env_config",
    autospec=True
)
def test_data_mapper_get_files_to_process(mock_get_env_config, mock_get_map_config, tmpdir):
    mock_get_map_config.return_value = [{"key": "value"}]
    mock_get_env_config.return_value = {
        "data_dir_base": [str(tmpdir), "base"],
        "data_dir_idl": [str(tmpdir), "idl"],
        "data_dir_arch": [str(tmpdir), "archive"]
    }
    tmp_file = tmpdir.mkdir("base").join("file_1.json")
    tmp_file.write("content")
    assert tmp_file.read() == "content"
    data_mapper = DataMapper(data_origin='fb')
    data_mapper.get_files_to_process()
    assert data_mapper.files_to_process == ['file_1.json']


@patch(
    "phoenix.map.config.config.get_map_config",
    autospec=True
)
@patch(
    "phoenix.map.config.config.get_env_config",
    autospec=True
)
def test_data_mapper_map_data_to_idl_files(
        mock_get_env_config,
        mock_get_map_config,
        tmpdir,
        fb_post_mock_input_data,
        fb_account_mock_output_data
):
    # mock returns
    mock_get_map_config.return_value = [{
        "data_origin": "fb",
        "data_category": "dim",
        "name": "d_facebook_accounts",
        "processing_steps": ["_normalize_nested_json"],
        "normalize_field": "account",
        "foreign_key_field": "account",
        "columns": ["src_file_name", "account"]
    }]
    mock_get_env_config.return_value = {
        "data_dir_base": [str(tmpdir), "base"],
        "data_dir_idl": [str(tmpdir), "idl"],
        "data_dir_arch": [str(tmpdir), "archive"]
    }
    # make temp dirs
    tmp_base_file = tmpdir.mkdir("base").join("file_1.json")
    tmp_base_file.write(fb_post_mock_input_data)
    # instantiate class object and run methods to test
    data_mapper = DataMapper(data_origin='fb')
    data_mapper.get_files_to_process()
    data_mapper.map_data_to_idl_files()
    # assert created file has correct content
    for file in data_mapper.output_files:
        with open(file) as output_file:
            assert output_file.read() == fb_account_mock_output_data
