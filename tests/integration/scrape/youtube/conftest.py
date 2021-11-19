"""Youtube integration conftest."""
import pytest

from phoenix.common import utils


@pytest.fixture
def mock_data_folder():
    """Mock data folder path."""
    return str(utils.relative_path("./data/", __file__)) + "/"


@pytest.fixture
def youtube_channel_config_url(mock_data_folder):
    """URL for the test youtube channel config."""
    return f"file:///{mock_data_folder}channels_config.csv"


def read_youtube_response(path):
    """Read the youtube response."""
    with open(path) as f:
        result = f.read()
    return result


@pytest.fixture
def youtube_channel_page_final(mock_data_folder):
    """Youtube Channel final page (no next page)."""
    return read_youtube_response(mock_data_folder + "channels_page_final.json")


@pytest.fixture
def youtube_channel_page_with_next(mock_data_folder):
    """Youtube Channel page with next page."""
    return read_youtube_response(mock_data_folder + "channels_page_with_next.json")


@pytest.fixture
def youtube_search_videos_page_final(mock_data_folder):
    """Youtube search videos final page (no next page)."""
    return read_youtube_response(mock_data_folder + "search_videos_page_final.json")


@pytest.fixture
def youtube_search_videos_page_with_next(mock_data_folder):
    """Youtube search videos page with next page."""
    return read_youtube_response(mock_data_folder + "search_videos_page_with_next.json")
