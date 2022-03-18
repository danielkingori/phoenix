"""Test Filter of the export manual scraping."""
import pandas as pd
import pytest

from phoenix.tag import export_manual_scraping


@pytest.fixture()
def input_dataframe():
    """Input dataframe for filtering."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "account_handle": ["account_1"] * 3 + ["account_2"] * 3 + ["account_3"] * 4,
            "has_topics": [True, False] * 5,
            "total_interactions": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )


@pytest.mark.parametrize(
    ("expected_return, expected_matched_accounts, head, include_accounts, has_topics"),
    [
        (
            pd.DataFrame(
                {
                    "id": [10, 9, 8, 7],
                    "account_handle": ["account_3"] * 4,
                    "has_topics": [False, True] * 2,
                    "total_interactions": [10, 9, 8, 7],
                },
                index=pd.Int64Index([9, 8, 7, 6], dtype="int64"),
            ),
            ["account_3", "account_2", "account_1"],
            4,
            None,
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [9, 7, 5, 3],
                    "account_handle": ["account_3"] * 2 + ["account_2", "account_1"],
                    "has_topics": [True] * 4,
                    "total_interactions": [9, 7, 5, 3],
                },
                index=pd.Int64Index([8, 6, 4, 2], dtype="int64"),
            ),
            ["account_3", "account_2", "account_1"],
            4,
            None,
            True,
        ),
        (
            pd.DataFrame(
                {
                    "id": [6, 5],
                    "account_handle": ["account_2"] * 2,
                    "has_topics": [False, True],
                    "total_interactions": [6, 5],
                },
                index=pd.Int64Index([5, 4], dtype="int64"),
            ),
            ["account_2"],
            2,
            ["account_2"],
            None,
        ),
        (
            pd.DataFrame(
                {
                    "id": [5],
                    "account_handle": ["account_2"],
                    "has_topics": [True],
                    "total_interactions": [5],
                },
                index=pd.Int64Index([4], dtype="int64"),
            ),
            ["account_2"],
            3,
            ["account_2"],
            True,
        ),
        (
            pd.DataFrame(
                {
                    "id": [5, 3],
                    "account_handle": ["account_2", "account_1"],
                    "has_topics": [True, True],
                    "total_interactions": [5, 3],
                },
                index=pd.Int64Index([4, 2], dtype="int64"),
            ),
            ["account_1", "account_2"],
            2,
            ["account_1", "account_2"],
            True,
        ),
        (
            pd.DataFrame(
                {
                    "id": [6, 5, 4, 3, 2],
                    "account_handle": ["account_2"] * 3 + ["account_1"] * 2,
                    "has_topics": [False, True, False, True, False],
                    "total_interactions": [6, 5, 4, 3, 2],
                },
                index=pd.Int64Index([5, 4, 3, 2, 1], dtype="int64"),
            ),
            ["account_1", "account_2"],
            5,
            ["account_1", "account_2"],
            False,
        ),
        (
            pd.DataFrame(
                {
                    "id": [6, 5],
                    "account_handle": ["account_2"] * 2,
                    "has_topics": [False, True],
                    "total_interactions": [6, 5],
                },
                index=pd.Int64Index([5, 4], dtype="int64"),
            ),
            ["account_1", "account_2"],
            2,
            ["account_1", "account_2"],
            False,
        ),
    ],
)
def test_filter(
    expected_return,
    expected_matched_accounts,
    head,
    include_accounts,
    has_topics,
    input_dataframe,
):
    """Test filter."""
    return_df, return_matched_accounts = export_manual_scraping.filter_posts(
        facebook_posts_df=input_dataframe,
        head=head,
        include_accounts=include_accounts,
        has_topics=has_topics,
    )
    pd.testing.assert_frame_equal(return_df, expected_return)
    assert return_matched_accounts.sort() == expected_matched_accounts.sort()
