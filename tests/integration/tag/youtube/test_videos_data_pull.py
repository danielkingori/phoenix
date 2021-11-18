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
    - including adding the filter columns
    - de-duplicates them based on run datetime
    - order by created_at descending
    - add file_timestamp
    - filter for month and date.
    """
    result = youtube_videos.execute(youtube_videos_source_folder_url, 2000, 1)
    expected = pd.DataFrame(
        [
            {
                "id": "video_3-id",
                "created_at": datetime.datetime(
                    2000, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
                ),
                "channel_id": "video_3-channel_id",
                "channel_title": "video_3-channel_title_updated",
                "title": "video_3-title_updated",
                "description": "video_3-description_updated",
                "text": "video_3-title_updated video_3-description_updated",
                "etag": "item_3-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 31),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 31,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "video_2-id",
                "created_at": datetime.datetime(
                    2000, 1, 2, 2, 26, 31, tzinfo=datetime.timezone.utc
                ),
                "channel_id": "video_2-channel_id",
                "channel_title": "video_2-channel_title_updated",
                "title": "video_2-title_updated",
                "description": "video_2-description_updated",
                "text": "video_2-title_updated video_2-description_updated",
                "etag": "item_2-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 2, 2, 26, 31, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 2),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 2,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "video_4-id",
                "created_at": datetime.datetime(2000, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
                "channel_id": "video_4-channel_id",
                "channel_title": "video_4-channel_title",
                "title": "video_4-title",
                "description": "video_4-description",
                "text": "video_4-title video_4-description",
                "etag": "item_2-response_2-etag_2",
                "response_etag": "response_2-etag_2",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 1),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 1,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "video_1-id",
                "created_at": datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
                "channel_id": "video_1-channel_id",
                "channel_title": "video_1-channel_title",
                "title": "video_1-title",
                "description": "video_1-description",
                "text": "video_1-title video_1-description",
                "etag": "item_2-response_1-etag_1",
                "response_etag": "response_1-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 1),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 1,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
                ),
            },
        ],
        index=pd.Int64Index([1, 2, 3, 4], dtype="int64"),
    )
    pd.testing.assert_frame_equal(result, expected)
