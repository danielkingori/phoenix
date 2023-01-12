"""Test channels functionality."""
import mock
import pandas as pd
import pytest

from phoenix.scrape.youtube import channels_config


@mock.patch("phoenix.scrape.youtube.channels_config._get_channels_config")
def test_get_channels_to_scrape_channel_id_error(m_get_channels_config):
    """Test get_channels_to_scrape."""
    config_url = "config_url"
    m_get_channels_config.return_value = pd.DataFrame(
        {"not_channel_id": ["channel_id_1", "channel_id_2"]}
    )

    with pytest.raises(RuntimeError) as error:
        channels_config.get_channels_to_scrape(config_url)
        assert config_url in str(error.value)
