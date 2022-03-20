"""Test Filter of the export manual scraping."""
import datetime

import pandas as pd
import pytest

from phoenix.tag import export_manual_scraping


DEC_2021 = datetime.datetime(2021, 12, 31, 11, 59, 59)
JAN_2022 = datetime.datetime(2022, 1, 1, 0, 0, 1)
FEB_2022 = datetime.datetime(2022, 2, 1, 0, 0, 1)
JAN_2022_FILTER = datetime.datetime(2022, 1, 1, 0, 0, 0)
FEB_2022_FILTER = datetime.datetime(2022, 2, 1, 0, 0, 0)


@pytest.fixture()
def input_dataframe():
    """Input dataframe for filtering."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "account_platform_id": [1] * 3 + [2] * 3 + [3] * 4,
            "account_handle": ["account_1"] * 3 + ["account_2"] * 3 + ["account_3"] * 4,
            "account_name": ["account_1"] * 3 + ["account_2"] * 3 + ["account_3"] * 4,
            "has_topics": [True, False] * 5,
            "total_interactions": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "timestamp_filter": [
                DEC_2021,
                JAN_2022,
                FEB_2022,
                DEC_2021,
                JAN_2022,
                FEB_2022,
                DEC_2021,
                JAN_2022,
                JAN_2022,
                FEB_2022,
            ],
        }
    )


@pytest.mark.parametrize(
    (
        "expected_return, expected_matched_accounts,"
        " head, include_accounts, has_topics, after_timestamp, before_timestamp"
    ),
    [
        (
            pd.DataFrame(
                {
                    "id": [10, 9, 8, 7],
                    "account_platform_id": [3] * 4,
                    "account_handle": ["account_3"] * 4,
                    "account_name": ["account_3"] * 4,
                    "has_topics": [False, True] * 2,
                    "total_interactions": [10, 9, 8, 7],
                    "timestamp_filter": [
                        FEB_2022,
                        JAN_2022,
                        JAN_2022,
                        DEC_2021,
                    ],
                },
                index=pd.Int64Index([9, 8, 7, 6], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [3, 2, 1],
                    "account_handle": ["account_3", "account_2", "account_1"],
                    "account_name": ["account_3", "account_2", "account_1"],
                },
            ),
            4,
            None,
            None,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [9, 7, 5, 3],
                    "account_platform_id": [3] * 2 + [2, 1],
                    "account_handle": ["account_3"] * 2 + ["account_2", "account_1"],
                    "account_name": ["account_3"] * 2 + ["account_2", "account_1"],
                    "has_topics": [True] * 4,
                    "total_interactions": [9, 7, 5, 3],
                    "timestamp_filter": [
                        JAN_2022,
                        DEC_2021,
                        JAN_2022,
                        FEB_2022,
                    ],
                },
                index=pd.Int64Index([8, 6, 4, 2], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [3, 2, 1],
                    "account_handle": ["account_3", "account_2", "account_1"],
                    "account_name": ["account_3", "account_2", "account_1"],
                },
            ),
            4,
            None,
            True,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [6, 5],
                    "account_platform_id": [2] * 2,
                    "account_handle": ["account_2"] * 2,
                    "account_name": ["account_2"] * 2,
                    "has_topics": [False, True],
                    "total_interactions": [6, 5],
                    "timestamp_filter": [
                        FEB_2022,
                        JAN_2022,
                    ],
                },
                index=pd.Int64Index([5, 4], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [2],
                    "account_handle": ["account_2"],
                    "account_name": ["account_2"],
                },
            ),
            2,
            [2],
            None,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [5],
                    "account_platform_id": [2],
                    "account_handle": ["account_2"],
                    "account_name": ["account_2"],
                    "has_topics": [True],
                    "total_interactions": [5],
                    "timestamp_filter": [
                        JAN_2022,
                    ],
                },
                index=pd.Int64Index([4], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [2],
                    "account_handle": ["account_2"],
                    "account_name": ["account_2"],
                },
            ),
            3,
            [2],
            True,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [5, 3],
                    "account_platform_id": [2, 1],
                    "account_handle": ["account_2", "account_1"],
                    "account_name": ["account_2", "account_1"],
                    "has_topics": [True, True],
                    "total_interactions": [5, 3],
                    "timestamp_filter": [
                        JAN_2022,
                        FEB_2022,
                    ],
                },
                index=pd.Int64Index([4, 2], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [2, 1],
                    "account_handle": ["account_2", "account_1"],
                    "account_name": ["account_2", "account_1"],
                },
            ),
            2,
            [1, 2],
            True,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [6, 5, 4, 3, 2],
                    "account_platform_id": [2] * 3 + [1] * 2,
                    "account_handle": ["account_2"] * 3 + ["account_1"] * 2,
                    "account_name": ["account_2"] * 3 + ["account_1"] * 2,
                    "has_topics": [False, True, False, True, False],
                    "total_interactions": [6, 5, 4, 3, 2],
                    "timestamp_filter": [
                        FEB_2022,
                        JAN_2022,
                        DEC_2021,
                        FEB_2022,
                        JAN_2022,
                    ],
                },
                index=pd.Int64Index([5, 4, 3, 2, 1], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [2, 1],
                    "account_handle": ["account_2", "account_1"],
                    "account_name": ["account_2", "account_1"],
                },
            ),
            5,
            [1, 2],
            False,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [6, 5],
                    "account_platform_id": [2] * 2,
                    "account_handle": ["account_2"] * 2,
                    "account_name": ["account_2"] * 2,
                    "has_topics": [False, True],
                    "total_interactions": [6, 5],
                    "timestamp_filter": [
                        FEB_2022,
                        JAN_2022,
                    ],
                },
                index=pd.Int64Index([5, 4], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [2, 1],
                    "account_handle": ["account_2", "account_1"],
                    "account_name": ["account_2", "account_1"],
                },
            ),
            2,
            [1, 2],
            False,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
                    "account_platform_id": [3] * 4 + [2] * 3 + [1] * 3,
                    "account_handle": ["account_3"] * 4 + ["account_2"] * 3 + ["account_1"] * 3,
                    "account_name": ["account_3"] * 4 + ["account_2"] * 3 + ["account_1"] * 3,
                    "has_topics": [False, True] * 5,
                    "total_interactions": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
                    "timestamp_filter": [
                        FEB_2022,
                        JAN_2022,
                        JAN_2022,
                        DEC_2021,
                        FEB_2022,
                        JAN_2022,
                        DEC_2021,
                        FEB_2022,
                        JAN_2022,
                        DEC_2021,
                    ],
                },
                index=pd.Int64Index([9, 8, 7, 6, 5, 4, 3, 2, 1, 0], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [3, 2, 1],
                    "account_handle": ["account_3", "account_2", "account_1"],
                    "account_name": ["account_3", "account_2", "account_1"],
                },
            ),
            10,
            None,
            None,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [9, 8, 7, 5, 4, 2, 1],
                    "account_platform_id": [3] * 3 + [2] * 2 + [1] * 2,
                    "account_handle": ["account_3"] * 3 + ["account_2"] * 2 + ["account_1"] * 2,
                    "account_name": ["account_3"] * 3 + ["account_2"] * 2 + ["account_1"] * 2,
                    "has_topics": [True, False, True, True, False, False, True],
                    "total_interactions": [9, 8, 7, 5, 4, 2, 1],
                    "timestamp_filter": [
                        JAN_2022,
                        JAN_2022,
                        DEC_2021,
                        JAN_2022,
                        DEC_2021,
                        JAN_2022,
                        DEC_2021,
                    ],
                },
                index=pd.Int64Index([8, 7, 6, 4, 3, 1, 0], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [3, 2, 1],
                    "account_handle": ["account_3", "account_2", "account_1"],
                    "account_name": ["account_3", "account_2", "account_1"],
                },
            ),
            10,
            None,
            None,
            None,
            FEB_2022_FILTER,
        ),
        (
            pd.DataFrame(
                {
                    "id": [10, 9, 8, 6, 5, 3, 2],
                    "account_platform_id": [3] * 3 + [2] * 2 + [1] * 2,
                    "account_handle": ["account_3"] * 3 + ["account_2"] * 2 + ["account_1"] * 2,
                    "account_name": ["account_3"] * 3 + ["account_2"] * 2 + ["account_1"] * 2,
                    "has_topics": [False, True, False, False, True, True, False],
                    "total_interactions": [10, 9, 8, 6, 5, 3, 2],
                    "timestamp_filter": [
                        FEB_2022,
                        JAN_2022,
                        JAN_2022,
                        FEB_2022,
                        JAN_2022,
                        FEB_2022,
                        JAN_2022,
                    ],
                },
                index=pd.Int64Index([9, 8, 7, 5, 4, 2, 1], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [3, 2, 1],
                    "account_handle": ["account_3", "account_2", "account_1"],
                    "account_name": ["account_3", "account_2", "account_1"],
                },
            ),
            10,
            None,
            None,
            JAN_2022_FILTER,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [9, 8],
                    "account_platform_id": [3] * 2,
                    "account_handle": ["account_3"] * 2,
                    "account_name": ["account_3"] * 2,
                    "has_topics": [True, False],
                    "total_interactions": [9, 8],
                    "timestamp_filter": [
                        JAN_2022,
                        JAN_2022,
                    ],
                },
                index=pd.Int64Index([8, 7], dtype="int64"),
            ),
            pd.DataFrame(
                {
                    "account_platform_id": [3],
                    "account_handle": ["account_3"],
                    "account_name": ["account_3"],
                },
            ),
            10,
            [3],
            None,
            JAN_2022_FILTER,
            FEB_2022_FILTER,
        ),
    ],
)
def test_filter(
    expected_return,
    expected_matched_accounts,
    head,
    include_accounts,
    has_topics,
    after_timestamp,
    before_timestamp,
    input_dataframe,
):
    """Test filter."""
    return_df, return_matched_accounts = export_manual_scraping.filter_posts(
        facebook_posts_df=input_dataframe,
        head=head,
        include_accounts=include_accounts,
        has_topics=has_topics,
        after_timestamp=after_timestamp,
        before_timestamp=before_timestamp,
    )
    pd.testing.assert_frame_equal(return_df, expected_return)
    pd.testing.assert_frame_equal(return_matched_accounts, expected_matched_accounts)
