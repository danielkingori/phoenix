"""Test Youtube videos data puller."""
import datetime

import pandas as pd
import pytest

from phoenix.tag import data_pull
from phoenix.tag.data_pull import youtube_videos_pull as youtube_videos


@pytest.fixture()
def processed_videos_df() -> pd.DataFrame:
    """Fixture for dataframe processed from scraped videos JSON."""
    df = pd.DataFrame(
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
                "video_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_3-id",
                "channel_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_3-channel_id",
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
                "video_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_2-id",
                "channel_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_2-channel_id",
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
                "video_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_4-id",
                "channel_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_4-channel_id",
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
                "video_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_1-id",
                "channel_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_1-channel_id",
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
    return df


def test_videos_data_pull(youtube_raw_data_source_folder_url, processed_videos_df):
    """Integration test for the data pull.

    - Takes 2 test data source files
    - normalises them in to youtube_videos schema
    - including adding the filter columns
    - de-duplicates them based on run datetime
    - order by created_at descending
    - add file_timestamp
    - filter for month and date.
    """
    result = youtube_videos.from_json(youtube_raw_data_source_folder_url, 2000, 1)
    pd.testing.assert_frame_equal(result, processed_videos_df)


def test_videos_for_tagging(processed_videos_df):
    """Integration test for transforming processed videos ready for tagging (labelling) format."""
    result = youtube_videos.for_tagging(processed_videos_df)
    expected = pd.DataFrame(
        [
            {
                "object_id": "video_3-id",
                "text": "video_3-title_updated video_3-description_updated",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_VIDEO,
                "created_at": datetime.datetime(
                    2000, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
                ),
                "object_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_3-id",
                "object_user_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_3-channel_id",
                "object_user_name": "video_3-channel_title_updated",
            },
            {
                "object_id": "video_2-id",
                "text": "video_2-title_updated video_2-description_updated",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_VIDEO,
                "created_at": datetime.datetime(
                    2000, 1, 2, 2, 26, 31, tzinfo=datetime.timezone.utc
                ),
                "object_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_2-id",
                "object_user_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_2-channel_id",
                "object_user_name": "video_2-channel_title_updated",
            },
            {
                "object_id": "video_4-id",
                "text": "video_4-title video_4-description",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_VIDEO,
                "created_at": datetime.datetime(2000, 1, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
                "object_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_4-id",
                "object_user_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_4-channel_id",
                "object_user_name": "video_4-channel_title",
            },
            {
                "object_id": "video_1-id",
                "text": "video_1-title video_1-description",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_VIDEO,
                "created_at": datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
                "object_url": youtube_videos.YOUTUBE_VIDEOS_URL + "video_1-id",
                "object_user_url": youtube_videos.YOUTUBE_CHANNEL_URL + "video_1-channel_id",
                "object_user_name": "video_1-channel_title",
            },
        ],
        index=pd.Index(
            ["video_3-id", "video_2-id", "video_4-id", "video_1-id"],
            dtype="object",
            name="object_id",
        ),
    )
    pd.testing.assert_frame_equal(result, expected)
