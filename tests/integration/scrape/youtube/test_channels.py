"""Test channels functionality."""
import json

import pytest

from phoenix.scrape.youtube import channels, channels_config


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
