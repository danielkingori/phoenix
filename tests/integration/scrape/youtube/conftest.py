"""Youtube integration conftest."""
import pytest

from phoenix.common import utils


@pytest.fixture
def youtube_channel_config_url():
    """URL for the test youtube channel config."""
    config_path = utils.relative_path("./channels_config.csv", __file__)
    return f"file:///{config_path}"
