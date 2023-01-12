"""Test channels functionality."""
import json
import os

import googleapiclient
import mock
import pytest

from phoenix.scrape.youtube import channels, channels_config, utils


@pytest.mark.auth
def test_get_channels_auth(youtube_channel_config_url):
    """Test get_channels_to_scrape Authenticated.

    This makes a real request to the YouTube API.
    """
    cur_channels_config = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    result = channels.get_channels(cur_channels_config)
    assert len(result) == 1
    # Build Up should be one of the titles on the list
    assert "Build Up" in json.dumps(result)
    # If you want to persist the response for mock testing
    #  with open("response.json", "w") as io:
    #     json.dump(result[0], io, indent=4)


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
def test_get_channels_1_page(youtube_channel_config_url, youtube_channel_page_final):
    """Test get_channels with mocked data for one page."""
    header_success = {"status": "200"}
    http = googleapiclient.http.HttpMockSequence([(header_success, youtube_channel_page_final)])
    cur_channels_config = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    client = utils.get_client(http_mock=http)
    result = channels.get_channels(cur_channels_config, client=client)
    assert len(result) == 1
    # Build Up should be one of the titles on the list
    assert "Build Up" in json.dumps(result)
    assert result[0] == json.loads(youtube_channel_page_final)


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
def test_get_channels_2_page(
    youtube_channel_config_url, youtube_channel_page_final, youtube_channel_page_with_next
):
    """Test get_channels_to_scrape for two pages."""
    header_success = {"status": "200"}
    http = googleapiclient.http.HttpMockSequence(
        [
            (header_success, youtube_channel_page_with_next),
            (header_success, youtube_channel_page_final),
        ]
    )
    cur_channels_config = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    client = utils.get_client(http_mock=http)
    result = channels.get_channels(cur_channels_config, client=client)
    assert len(result) == 2
    # Build Up should be one of the titles on the list
    assert "Build Up" in json.dumps(result)
    assert result[0] == json.loads(youtube_channel_page_with_next)
    assert result[1] == json.loads(youtube_channel_page_final)
