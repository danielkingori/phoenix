"""Test channels functionality."""
from phoenix.scrape.youtube import channels_config


def test_get_channels_to_scrape(youtube_channel_config_url):
    """Test get_channels_to_scrape."""
    result = channels_config.get_channels_to_scrape(youtube_channel_config_url)
    assert "channel_id" in result.columns
    assert result["channel_id"].dtype.kind == "O"
    assert result.shape[0] == 2
