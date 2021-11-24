"""Testing configuration."""
import pytest

from phoenix.common import utils


@pytest.fixture()
def youtube_raw_data_source_folder_url():
    """YouTube raw data source folder URL."""
    rel_path = utils.relative_path("./data/", __file__)
    return f"file://{rel_path}/"
