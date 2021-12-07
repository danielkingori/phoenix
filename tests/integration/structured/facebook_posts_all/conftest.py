"""Conftest."""
import datetime

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def expected_facebook_posts_all():
    """Expected facebook_posts_all."""
    return pd.DataFrame(
        {
            "platform_id": ["11430503000_10159078346893000", "21430503000_20159078346893000"],
            "platform": ["Facebook", "Facebook"],
            "type": ["photo", "photo"],
            "text": ["message_1", "message_2"],
            "link": ["link_1", "link_2"],
            "post_url": [
                "https://www.facebook.com/account_1/posts/post_1",
                "https://www.facebook.com/account_2/posts/post_2",
            ],
            "subscriber_count": [100, 100],
            "total_interactions": [101.0, 101.0],
            "id": ["10000|10159078346893000", "20000|20159078346893000"],
            "account_name": ["account_name_1", "account_name_2"],
            "account_handle": ["account_handle_1", "account_handle_2"],
            "account_platform_id": [1, 2],
            "account_page_category": ["account_category_1", "account_category_2"],
            "account_page_admin_top_country": ["US", "US"],
            "account_page_description": ["account_description_1", "account_description_2"],
            "account_url": ["account_url_1", "account_url_2"],
            "account_page_created_date": [
                datetime.datetime(2019, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2019, 2, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            ],
            "statistics_actual_like_count": [42617, 1],
            "statistics_actual_comment_count": [1060, 3],
            "statistics_actual_share_count": [303, 2],
            "statistics_actual_love_count": [10937, 4],
            "statistics_actual_wow_count": [35, 5],
            "statistics_actual_haha_count": [123, 6],
            "statistics_actual_sad_count": [4, 7],
            "statistics_actual_angry_count": [2, 8],
            "statistics_actual_care_count": [520, 0],
            "overperforming_score": [np.nan, np.nan],
            "interaction_rate": [np.nan, np.nan],
            "underperforming_score": [np.nan, np.nan],
            "created_at": [
                datetime.datetime(2020, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2020, 2, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            ],
            "timestamp_filter": [
                datetime.datetime(2020, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2020, 2, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            ],
            "date_filter": [datetime.date(2020, 1, 1), datetime.date(2020, 2, 1)],
            "year_filter": [2020, 2020],
            "month_filter": [1, 2],
            "day_filter": [1, 1],
            "updated_at": [
                datetime.datetime(2020, 1, 2, 1, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2020, 2, 2, 1, 1, 1, tzinfo=datetime.timezone.utc),
            ],
            "scrape_url": [
                "https://mbasic.facebook.com/account_1/posts/post_1",
                "https://mbasic.facebook.com/account_2/posts/post_2",
            ],
            "url_post_id": ["1", "2"],
            "file_timestamp": [
                datetime.datetime(2021, 2, 2, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2021, 2, 2, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            ],
            "file_base": [
                "raw_data_1-20210202T010101.000001Z.json",
                "raw_data_1-20210202T010101.000001Z.json",
            ],
        }
    )
