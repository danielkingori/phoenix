"""Test comment threads scraping."""
import json
import os

import googleapiclient
import mock
import pytest

from phoenix.scrape.youtube import channels_config, comment_threads, utils
from tests.integration.scrape.youtube import conftest


@pytest.mark.auth
def test_get_comment_threads_for_channel_live_with_auth(youtube_channel_config_url):
    """Test get comment threads for channel - live call and requires being authenticated.

    This makes a real request to the YouTube API.
    """
    cur_channels_config = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    result = comment_threads.get_comment_threads(cur_channels_config["channel_id"][0], max_pages=2)
    # If you want to persist the response for mock testing
    # with open("response.json", "w") as io:
    #    json.dump(result, io, indent=4)
    assert len(result) > 0
    json_result = json.dumps(result)
    assert "youtube#commentThreadListResponse" in json_result
    assert "youtube#commentThread" in json_result
    assert "topLevelComment" in json_result
    assert "youtube#comment" in json_result


@pytest.fixture
def youtube_comment_threads_page(mock_data_folder):
    """Youtube comment threads response."""
    return conftest.read_youtube_response(mock_data_folder + "comment_threads_response.json")


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
def test_get_comment_threads_for_channel(youtube_comment_threads_page):
    """Test get_comment_threads with mocked data for one page."""
    http = googleapiclient.http.HttpMockSequence(
        [({"status": "200"}, youtube_comment_threads_page)]
    )
    client = utils.get_client(http_mock=http)
    result = comment_threads.get_comment_threads("channel_id", max_pages=2, client=client)
    assert len(result) == 1
    assert result[0] == json.loads(youtube_comment_threads_page)


@mock.patch.dict(os.environ, {utils.API_KEY_ENV_NAME: "key"})
@mock.patch("phoenix.scrape.youtube.comment_threads.get_comment_threads")
def test_get_comment_threads_for_channel_config(
    mocked_get_comment_threads, youtube_channel_config_url
):
    """Test get_comment_threads_for_channel_config."""
    mocked_get_comment_threads.return_value = [{"mock": "data"}]
    cur_channels_config = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    result = comment_threads.get_comment_threads_for_channels_config(cur_channels_config)
    assert len(result) == 2
    mocked_get_comment_threads.assert_has_calls(
        [
            mock.call(cur_channels_config["channel_id"][0]),
            mock.call(cur_channels_config["channel_id"][1]),
        ]
    )
