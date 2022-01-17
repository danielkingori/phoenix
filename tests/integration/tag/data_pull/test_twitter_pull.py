"""Test Twitter tweets data puller (processing of raw scraped data into structured data)."""
import datetime

import numpy as np
import pandas as pd
import pytest

from phoenix.tag import data_pull


@pytest.fixture()
def processed_df() -> pd.DataFrame:
    """Fixture for dataframe processed from scraped tweets JSON."""
    return pd.DataFrame(
        [
            {
                "id_str": "1461731209554313216",
                "created_at": datetime.datetime(
                    2021, 11, 19, 16, 20, 52, tzinfo=datetime.timezone.utc
                ),
                "id": 1461731209554313223,
                "text": "tweet_1_text",
                "truncated": False,
                "display_text_range": [0, 140],
                "metadata": {"iso_language_code": "ar", "result_type": "recent"},
                "source": (
                    '<a href="http://twitter.com/download/iphone" '
                    'rel="nofollow">Twitter for iPhone</a>'
                ),
                "in_reply_to_status_id": None,
                "in_reply_to_status_id_str": None,
                "in_reply_to_user_id": None,
                "in_reply_to_user_id_str": None,
                "in_reply_to_screen_name": None,
                "geo": np.nan,
                "coordinates": np.nan,
                "contributors": np.nan,
                "is_quote_status": False,
                "retweet_count": 4,
                "favorite_count": 0,
                "favorited": False,
                "retweeted": False,
                "language_from_api": "ar",
                "file_timestamp": datetime.datetime(
                    2021, 11, 19, 17, 56, 15, 539674, tzinfo=datetime.timezone.utc
                ),
                "timestamp_filter": datetime.datetime(
                    2021, 11, 19, 16, 20, 52, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2021, 11, 19),
                "year_filter": 2021,
                "month_filter": 11,
                "day_filter": 19,
                "medium_type": "text",
                "platform_media_type": None,
                "url_within_text": None,
                "user_id": 1455531969186607120,
                "user_id_str": "1455531969186607120",
                "user_name": "tweet_1_name_2",
                "user_screen_name": "E0S1msEvEzIopEc",
                "user_location": "",
                "user_description": "tweet_1_desc_2",
                "user_url": None,
                "user_protected": False,
                "user_created_at": datetime.datetime(
                    2021, 11, 19, 16, 20, 52, tzinfo=datetime.timezone.utc
                ),
                "user_geo_enabled": False,
                "user_verified": False,
                "user_lang": None,
            },
            {
                "id_str": "1461749682200358912",
                "created_at": datetime.datetime(
                    2021, 11, 19, 17, 34, 16, tzinfo=datetime.timezone.utc
                ),
                "id": 1461749682200358914,
                "text": "tweet_2_text",
                "truncated": False,
                "display_text_range": [12, 35],
                "metadata": {"iso_language_code": "und", "result_type": "recent"},
                "source": (
                    '<a href="http://twitter.com/download/android" '
                    'rel="nofollow">Twitter for Android</a>'
                ),
                "in_reply_to_status_id": 1.461456116790014e18,
                "in_reply_to_status_id_str": 1.461456116790014e18,
                "in_reply_to_user_id": 7.936029066038108e17,
                "in_reply_to_user_id_str": 7.936029066038108e17,
                "in_reply_to_screen_name": "makhlafi22",
                "geo": np.nan,
                "coordinates": np.nan,
                "contributors": np.nan,
                "is_quote_status": False,
                "retweet_count": 0,
                "favorite_count": 0,
                "favorited": False,
                "retweeted": False,
                "language_from_api": "und",
                "file_timestamp": datetime.datetime(
                    2021, 11, 19, 17, 56, 15, 539674, tzinfo=datetime.timezone.utc
                ),
                "timestamp_filter": datetime.datetime(
                    2021, 11, 19, 17, 34, 16, tzinfo=datetime.timezone.utc
                ),
                "date_filter": datetime.date(2021, 11, 19),
                "year_filter": 2021,
                "month_filter": 11,
                "day_filter": 19,
                "medium_type": "text",
                "platform_media_type": None,
                "url_within_text": None,
                "user_id": 899624482616332289,
                "user_id_str": "899624482616332289",
                "user_name": "tweet_2_name_2",
                "user_screen_name": "alazzani_jmal",
                "user_location": "Yemen",
                "user_description": "tweet_2_desc",
                "user_url": None,
                "user_protected": False,
                "user_created_at": datetime.datetime(
                    2021, 11, 19, 17, 34, 16, tzinfo=datetime.timezone.utc
                ),
                "user_geo_enabled": False,
                "user_verified": False,
                "user_lang": None,
            },
        ]
    )


def test_data_pull(twitter_raw_data_source_folder_url, processed_df):
    """Integration test for data pull (processing) of twitter files."""
    result = data_pull.twitter_pull.twitter_json(twitter_raw_data_source_folder_url)
    pd.testing.assert_frame_equal(result, processed_df)
