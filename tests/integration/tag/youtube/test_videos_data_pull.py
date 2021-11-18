"""Test Youtube videos data puller."""
import datetime

import pandas as pd
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
    expected = pd.DataFrame([
        {
            "id": "video_1-id",
            "created_at": datetime.datetime(2000, 1, 1, 0, 0, 0),
            "channel_id": "video_1-channel_id",
            "title": "video_1-title",
            "description": "video_1-description",
            "text": "video_1-title video_1-description",
            "channel_title": "video_1-channel_title",
            "etag": "item_2-response_1-etag_1",
            "response_etag": "response_1-etag_1",
        },
        {
            "id": "video_4-id",
            "created_at": datetime.datetime(2000, 1, 1, 1, 0, 0),
            "channel_id": "video_4-channel_id",
            "title": "video_4-title",
            "description": "video_4-description",
            "text": "video_4-title video_4-description",
            "channel_title": "video_4-channel_title",
            "etag": "item_2-response_2-etag_2",
            "response_etag": "response_2-etag_2",
        },
        {
            "id": "video_2-id",
            "created_at": datetime.datetime(2000, 1, 2, 2, 26, 31),
            "channel_id": "video_2-channel_id",
            "title": "video_2-title_updated",
            "description": "video_2-description_updated",
            "text": "video_2-title-updated video_2-description_updated",
            "channel_title": "video_2-channel_title_updated",
            "etag": "item_3-response_1-etag_1",
            "response_etag": "response_1-etag_1",
        },
        {
            "id": "video_3-id",
            "created_at": datetime.datetime(2000, 1, 31, 23, 59, 59),
            "channel_id": "video_3-channel_id",
            "title": "video_3-title_updated",
            "description": "video_3-description_updated",
            "text": "video_3-title-updated video_3-description_updated",
            "channel_title": "video_3-channel_title_updated",
            "etag": "item_4-response_1-etag_1",
            "response_etag": "response_1-etag_1",
        },
    ])
    pd.testing.assert_frame_equal(result, expected)
