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


def test_permutations_remove_duplicates():
    """Test permutations_remove_duplicates makes permutations of a list."""
    items_list = ["Alice", "Bob", "Charlie", "Dan"]

    expected_permutation_list = [
        ("Alice", "Bob"),
        ("Alice", "Charlie"),
        ("Alice", "Dan"),
        ("Bob", "Charlie"),
        ("Bob", "Dan"),
        ("Charlie", "Dan"),
    ]

    actual_permutation_list = facebook_topic_pull.permutations_remove_duplicates(items_list)

    assert actual_permutation_list == expected_permutation_list


def test_get_weighted_dataframe():
    """Test get_weighted_dataframe."""
    input_data = pd.DataFrame(
        [
            ("Alice", "Bob"),
            ("Alice", "Bob"),
            ("Alice", "Bob"),
            ("Alice", "Charlie"),
            ("Alice", "Charlie"),
            ("Alice", "Charlie"),
            ("Alice", "Dan"),
            ("Bob", "Charlie"),
            ("Bob", "Dan"),
            ("Charlie", "Dan"),
            ("Charlie", "Dan"),
        ]
    )

    expected_df = pd.DataFrame(
        {
            "topic-1": ["Alice", "Alice", "Alice", "Bob", "Bob", "Charlie"],
            "topic-2": ["Bob", "Charlie", "Dan", "Charlie", "Dan", "Dan"],
            "weight": [3, 3, 1, 1, 1, 2],
        }
    )

    actual_df = facebook_topic_pull.get_weighted_dataframe(input_data)

    pd.testing.assert_frame_equal(actual_df, expected_df)
