"""Test Youtube comments data puller."""
import datetime

import pandas as pd
import pytest

from phoenix.tag import data_pull


@pytest.fixture()
def processed_comments_df() -> pd.DataFrame:
    """Fixture for dataframe processed from scraped comment threads JSON."""
    df = pd.DataFrame(
        [
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
