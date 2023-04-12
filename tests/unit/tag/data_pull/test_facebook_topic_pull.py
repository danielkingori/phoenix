"""Test facebook_topic_pull."""

import numpy as np
import pandas as pd
import pytest

from phoenix.tag.data_pull import facebook_topic_pull


def test_get_unique_groups_na():
    """Test get_unique_groups removes NA topics from the groups."""
    input_df = pd.DataFrame(
        {
            "user_name": ["Alice", "Bob", "Bob", "Alice", "Alice", "Alice", "Alice"],
            "topic": [
                "apples",
                "apples",
                "cherries",
                "apples",
                "bananas",
                "dates",
                np.nan,
            ],
        }
    )

    expected_series = pd.Series(
        index=["Alice", "Bob"],
        data=[
            ["apples", "bananas", "dates"],
            ["apples", "cherries"],
        ],
        name="topic",
    )

    expected_series.index.names = ["user_name"]

    result_series = facebook_topic_pull.get_unique_groups(input_df)

    pd.testing.assert_series_equal(result_series, expected_series)


@pytest.mark.parametrize("groupby_key", [("user_name"), ("account_name")])
def test_get_unique_groups(groupby_key):
    """Test unique sets of topics are made per group_by key."""
    input_df = pd.DataFrame(
        {
            groupby_key: ["Alice", "Bob", "Bob", "Alice", "Alice", "Alice", "Alice"],
            "topic": [
                "apples",
                "apples",
                "cherries",
                "apples",
                "bananas",
                "dates",
                "dates",
            ],
        }
    )

    expected_series = pd.Series(
        index=["Alice", "Bob"],
        data=[
            ["apples", "bananas", "dates"],
            ["apples", "cherries"],
        ],
        name="topic",
    )

    expected_series.index.names = [groupby_key]

    result_series = facebook_topic_pull.get_unique_groups(input_df)

    pd.testing.assert_series_equal(result_series, expected_series)
