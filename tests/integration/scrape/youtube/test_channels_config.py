"""Test channels functionality."""
from phoenix.common import utils
from phoenix.scrape.youtube import channels_config


def test_get_channels_to_scrape():
    """Test get_channels_to_scrape."""
    config_path = utils.relative_path("./channels_config.csv", __file__)
    config_url = f"file:///{config_path}"
    result = channels_config.get_channels_to_scrape(config_url)
    assert "channel_id" in result.columns
    assert result["channel_id"].dtype.kind == "O"
    assert result.shape[0] == 2
