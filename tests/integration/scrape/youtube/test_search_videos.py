"""Test search functionality."""
import datetime
import json
import logging
import os

import googleapiclient
import mock
import pytest

from phoenix.scrape.youtube import channels_config
from phoenix.scrape.youtube import search_videos as search
from phoenix.scrape.youtube import utils


@pytest.mark.auth
def test_get_videos_for_channel_defaults_auth(youtube_channel_config_url):
    """Test get videos for channel Authenticated.

    You may have to change the number of days to go back to get a correct result.

    This makes a real request to the YouTube API.
    """
    cur_channels_config = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    published_after = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)
    logging.info(published_after)
    result = search.get_videos_for_channel(cur_channels_config["channel_id"][0], published_after)
    # There should always be a result even if there is no videos found
    assert len(result) > 0
    # Build Up should be one of the titles on the list
    assert "youtube#searchListResponse" in json.dumps(result)
    assert "youtube#video" in json.dumps(result)
    assert "youtube#channel" not in json.dumps(result)
    assert "youtube#playlist" not in json.dumps(result)
    # If you want to persist the response for mock testing
    #  with open("response.json", "w") as io:
    #      json.dump(result[0], io, indent=4)


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
def test_get_videos_for_channel(youtube_search_videos_page_final):
    """Test get_videos with mocked data for one page."""
    header_success = {"status": "200"}
    http = googleapiclient.http.HttpMockSequence(
        [(header_success, youtube_search_videos_page_final)]
    )
    client = utils.get_client(http_mock=http)
    published_after = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=3)
    result = search.get_videos_for_channel("channel_id", published_after, None, None, client)
    assert len(result) == 1
    # Build Up should be one of the titles on the list
    assert result[0] == json.loads(youtube_search_videos_page_final)


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
def test_get_videos_for_channel_2_page(
    youtube_search_videos_page_final, youtube_search_videos_page_with_next
):
    """Test get videos with mocked data for two pages."""
    header_success = {"status": "200"}
    http = googleapiclient.http.HttpMockSequence(
        [
            (header_success, youtube_search_videos_page_with_next),
            (header_success, youtube_search_videos_page_final),
        ]
    )
    client = utils.get_client(http_mock=http)
    published_after = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=3)
    result = search.get_videos_for_channel("channel_id", published_after, None, None, client)
    assert len(result) == 2
    # Build Up should be one of the titles on the list
    assert result[0] == json.loads(youtube_search_videos_page_with_next)
    assert result[1] == json.loads(youtube_search_videos_page_final)
