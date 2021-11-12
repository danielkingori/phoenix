"""Test Utils."""
import os

import googleapiclient
import mock
import pytest

from phoenix.scrape.youtube import utils


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
def test_get_api_key_from_env():
    """Test the api key from env."""
    assert "key" == utils.get_api_key_from_env()


@mock.patch.dict(os.environ, {}, clear=True)
def test_get_api_key_from_env_not_found():
    """Test the api key from env not found error."""
    with pytest.raises(RuntimeError) as error:
        utils.get_api_key_from_env()
    assert utils.API_KEY_ENV_NAME in str(error.value)


@mock.patch("phoenix.scrape.youtube.utils.get_api_key_from_env")
def test_get_client(m_get_api_key):
    """Test get_client."""
    api_key = "api_key"
    m_get_api_key.return_value = api_key
    http_mock = googleapiclient.http.HttpMock()
    client = utils.get_client(http_mock)
    m_get_api_key.assert_called_once_with()
    assert isinstance(client, googleapiclient.discovery.Resource)
    assert client._developerKey == api_key
    assert client._baseUrl == "https://youtube.googleapis.com/"
