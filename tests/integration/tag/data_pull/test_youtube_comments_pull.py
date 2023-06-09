"""Test Youtube comments data puller."""
import datetime

import pandas as pd
import pytest

from phoenix.tag import data_pull


@pytest.fixture()
def processed_comments_df() -> pd.DataFrame:
    """Fixture for dataframe processed from scraped comment threads JSON."""
    return pd.DataFrame(
        [
            {
                "id": "comment_id_6",
                "published_at": datetime.datetime(
                    2000, 1, 21, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "updated_at": datetime.datetime(
                    2000, 1, 21, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "text": "comment_6-text_display",
                "text_original": "comment_6-text_original",
                "like_count": 0,
                "is_top_level_comment": False,
                "total_reply_count": None,
                "parent_comment_id": "comment_id_1",
                "author_channel_id": "comment_6-author_channel_id",
                "author_display_name": "comment_6-author_display_name",
                "channel_id": "channel_id_6",
                "video_id": "video_id_6",
                "video_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_6",
                "etag": "comment_6-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 21, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 21),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 21,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "comment_id_5",
                "published_at": datetime.datetime(
                    2000, 1, 20, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "updated_at": datetime.datetime(
                    2000, 1, 22, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "text": "comment_5-text_display",
                "text_original": "comment_5-text_original",
                "like_count": 0,
                "is_top_level_comment": False,
                "total_reply_count": None,
                "parent_comment_id": "comment_id_3",
                "author_channel_id": None,
                "author_display_name": "comment_5-author_display_name",
                "channel_id": "channel_id_3",
                "video_id": "video_id_3",
                "video_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_3",
                "etag": "comment_5-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 20, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 20),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 20,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "comment_id_4",
                "published_at": datetime.datetime(
                    2000, 1, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "updated_at": datetime.datetime(
                    2000, 1, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "text": "comment_4-text_display",
                "text_original": "comment_4-text_original",
                "like_count": 3,
                "is_top_level_comment": False,
                "total_reply_count": None,
                "parent_comment_id": "comment_id_3",
                "author_channel_id": "comment_4-author_channel_id",
                "author_display_name": "comment_4-author_display_name",
                "channel_id": "channel_id_3",
                "video_id": "video_id_3",
                "video_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_3",
                "etag": "comment_4-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 10),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 10,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "comment_id_3",
                "published_at": datetime.datetime(
                    2000, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "updated_at": datetime.datetime(2000, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc),
                "text": "comment_3-text_display",
                "text_original": "comment_3-text_original",
                "like_count": 2,
                "is_top_level_comment": True,
                "total_reply_count": 2,
                "parent_comment_id": None,
                "author_channel_id": "comment_3-author_channel_id",
                "author_display_name": "comment_3-author_display_name",
                "channel_id": "channel_id_3",
                "video_id": "video_id_3",
                "video_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_3",
                "etag": "comment_3-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 3),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 3,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
            {
                "id": "comment_id_1",
                "published_at": datetime.datetime(
                    2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "updated_at": datetime.datetime(2000, 1, 2, 0, 0, 0, tzinfo=datetime.timezone.utc),
                "text": "comment_1-text_display-updated",
                "text_original": "comment_1-text_original-updated",
                "like_count": 0,
                "is_top_level_comment": True,
                "total_reply_count": 1,
                "parent_comment_id": None,
                "author_channel_id": "comment_1-author_channel_id",
                "author_display_name": "comment_1-author_display_name",
                "channel_id": "channel_id_1",
                "video_id": "video_id_1",
                "video_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_1",
                "etag": "comment_1-response_2-etag_1",
                "response_etag": "response_2-etag_1",
                "timestamp_filter": datetime.datetime(
                    2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2000, 1, 1),
                "year_filter": 2000,
                "month_filter": 1,
                "day_filter": 1,
                "file_timestamp": datetime.datetime(
                    2000, 1, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc
                ),
            },
        ]
    )


def test_comments_data_pull(youtube_raw_data_source_folder_url, processed_comments_df):
    """Integration test for data pull (processing) of commentThreadListResponses files.

    - Takes 2 test data source files
    - Cases include:
        - Comment that has been updated between source files
        - Comment that has non-zero reply count but no replies included in response
        - Response list that contains no items
        - Comment that has received subsequent more replies between source files
        - Comment that is not in date range specified for filtering
        - Comment that doesn't have AuthorChannelId present as per
            https://developers.google.com/youtube/v3/docs/comments#resource
    - Normalises them to youtube_comments schema
    - Includes adding year, month, date filter columns
    - Add file_timestamp
    - De-duplicates them based on comment ID and latest runtime file
    - Order by published_at descending
    - Filter for month and date
    """
    result = data_pull.youtube_comments_pull.from_json(
        youtube_raw_data_source_folder_url + "comment_threads/", 2000, 1
    )
    pd.testing.assert_frame_equal(result, processed_comments_df)


def test_comments_for_tagging(processed_comments_df):
    """Integration test for transforming processed comments ready for tagging (labelling)."""
    result = data_pull.youtube_comments_pull.for_tagging(processed_comments_df)
    expected = pd.DataFrame(
        [
            {
                "object_id": "comment_id_6",
                "text": "comment_6-text_display",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_COMMENT,
                "created_at": datetime.datetime(
                    2000, 1, 21, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "object_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_6",
                "object_user_url": data_pull.constants.YOUTUBE_CHANNEL_URL
                + "comment_6-author_channel_id",
                "object_user_name": "comment_6-author_display_name",
                "object_parent_text": "comment_1-text_display-updated",
            },
            {
                "object_id": "comment_id_5",
                "text": "comment_5-text_display",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_COMMENT,
                "created_at": datetime.datetime(
                    2000, 1, 20, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "object_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_3",
                "object_user_url": None,
                "object_user_name": "comment_5-author_display_name",
                "object_parent_text": "comment_3-text_display",
            },
            {
                "object_id": "comment_id_4",
                "text": "comment_4-text_display",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_COMMENT,
                "created_at": datetime.datetime(
                    2000, 1, 10, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
                "object_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_3",
                "object_user_url": data_pull.constants.YOUTUBE_CHANNEL_URL
                + "comment_4-author_channel_id",
                "object_user_name": "comment_4-author_display_name",
                "object_parent_text": "comment_3-text_display",
            },
            {
                "object_id": "comment_id_3",
                "text": "comment_3-text_display",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_COMMENT,
                "created_at": datetime.datetime(2000, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc),
                "object_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_3",
                "object_user_url": data_pull.constants.YOUTUBE_CHANNEL_URL
                + "comment_3-author_channel_id",
                "object_user_name": "comment_3-author_display_name",
                "object_parent_text": None,
            },
            {
                "object_id": "comment_id_1",
                "text": "comment_1-text_display-updated",
                "object_type": data_pull.constants.OBJECT_TYPE_YOUTUBE_COMMENT,
                "created_at": datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
                "object_url": data_pull.constants.YOUTUBE_VIDEOS_URL + "video_id_1",
                "object_user_url": data_pull.constants.YOUTUBE_CHANNEL_URL
                + "comment_1-author_channel_id",
                "object_user_name": "comment_1-author_display_name",
                "object_parent_text": None,
            },
        ],
        index=pd.Index(
            ["comment_id_6", "comment_id_5", "comment_id_4", "comment_id_3", "comment_id_1"],
            dtype="object",
            name="object_id",
        ),
    )
    pd.testing.assert_frame_equal(result, expected)
