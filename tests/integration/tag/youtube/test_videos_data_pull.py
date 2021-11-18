"""Test Youtube videos data puller."""
import pytest

from phoenix.common import utils
from phoenix.tag.data_pull import youtube_videos


@pytest.fixture()
def youtube_videos_source_folder_url():
    """YouTube videos source folder URL."""
    rel_path = utils.relative_path("./data/", __file__)
    return f"file://{rel_path}/"


def test_videos_data_pull(youtube_videos_source_folder_url):
    """Integration test for the data pull.

    - Takes 2 test data source files
    - normalises them in to youtube_videos schema
    - de-duplicates them based on run datetime
    - filter for month and date.
    """
    result = youtube_videos.execute(youtube_videos_source_folder_url, 2000, 1)
    assert result.shape[0] != 0
