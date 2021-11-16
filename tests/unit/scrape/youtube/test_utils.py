"""Test Utils."""
import datetime
import os

import googleapiclient
import mock
import pytest
import pytz

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


@mock.patch("phoenix.scrape.youtube.utils.get_client")
def test_get_resource_client(m_get_client):
    """Test get_resource client."""
    client_key = "client_key"
    result = utils.get_resource_client(client_key)
    m_get_client.assert_called_once_with()
    m_get_client.return_value.client_key.assert_called_once_with()
    assert result == m_get_client.return_value.client_key.return_value


@mock.patch("phoenix.scrape.youtube.utils.get_client")
def test_get_resource_client_with_client(m_get_client):
    """Test get_resource client."""
    client_key = "client_key"
    m_client = mock.Mock()
    result = utils.get_resource_client(client_key, m_client)
    m_get_client.assert_not_called()
    m_client.client_key.assert_called_once_with()
    assert result == m_client.client_key.return_value


@pytest.mark.parametrize(
    "default_parts_list, parts_list, expected",
    [
        (["d1", "d2", "d3"], None, "d1,d2,d3"),
        (["d1", "d2", "d3"], ["g1", "g2"], "g1,g2"),
    ],
)
def test_get_part_str(default_parts_list, parts_list, expected):
    """Test get_part_str."""
    assert expected == utils.get_part_str(default_parts_list, parts_list)


@pytest.mark.parametrize(
    "dt, expected",
    [
        (
            datetime.datetime(2021, 1, 1, 2, 2, 2, 3, tzinfo=datetime.timezone.utc),
            "2021-01-01T02:02:02.000003+00:00",
        ),
        (
            datetime.datetime(2021, 1, 1, 2, 2, 2, tzinfo=datetime.timezone.utc),
            "2021-01-01T02:02:02+00:00",
        ),
    ],
)
def test_datetime_str(dt, expected):
    """Test datetime_str."""
    assert expected == utils.datetime_str(dt)


def test_disallowed_datetime_no_timezone():
    """Test disallowed datetimes."""
    dt = datetime.datetime.now()
    with pytest.raises(ValueError):
        utils.datetime_str(dt)


def test_disallowed_datetime_timezone():
    """Test disallowed datetimes."""
    timezone = pytz.timezone("America/Los_Angeles")
    dt = timezone.localize(datetime.datetime.now())
    with pytest.raises(ValueError):
        utils.datetime_str(dt)


def test_disallowed_datetime_timezone_st():
    """Test disallowed datetimes."""
    tz = datetime.timezone(datetime.timedelta(seconds=19800))
    dt = datetime.datetime.now().astimezone(tz)
    with pytest.raises(ValueError):
        utils.datetime_str(dt)
