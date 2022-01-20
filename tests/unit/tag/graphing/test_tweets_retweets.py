"""Tests for the twitter retweets graph."""

import pandas as pd
import pytest

from phoenix.tag.graphing import tweets_retweets


@pytest.fixture
def input_final_tweets_classes() -> pd.DataFrame:
    """Input dataframe of tweets with classes."""
    return pd.DataFrame(
        {
            "id": [2, 2, 1, 3, 4, 4, 5],
            "user_screen_name": ["u_1", "u_1", "u_2", "u_1", "u_3", "u_3", "u_3"],
            "retweeted_user_screen_name": [None, None, "u_1", "u_2", "u_1", "u_1", "u_1"],
            "class": ["foo", "foo", "c_1", "c_3", "c_1", "c_3", "c_2"],
        }
    )


@pytest.fixture
def input_final_tweets_accounts() -> pd.DataFrame:
    """Input dataframe of twitter accounts."""
    return pd.DataFrame(
        {
            "object_user_name": ["u_1", "u_2", "u_1", "u_4"],
            "account_label": ["acc_label_2", "acc_label_2", "acc_label_1", "acc_label_1"],
        }
    )


@pytest.fixture
def edges() -> pd.DataFrame:
    """Expected output edges dataframe."""
    return pd.DataFrame(
        {
            "tweeting_user_screen_name": ["u_1", "u_2", "u_3"],
            "retweeted_user_screen_name": ["u_2", "u_1", "u_1"],
            "times_retweeted": [1, 1, 2],
            "class": ["c_3", "c_1", "c_1, c_2, c_3"],
        }
    )


@pytest.fixture
def nodes() -> pd.DataFrame:
    """Expected output nodes dataframe."""
    return pd.DataFrame(
        {
            "user_screen_name": ["u_1", "u_2", "u_3", "u_4"],
            "account_label": ["acc_label_1, acc_label_2", "acc_label_2", None, "acc_label_1"],
        }
    )


def test_process(input_final_tweets_classes, input_final_tweets_accounts, edges, nodes):
    """Test processing inputs to edges and nodes."""
    output_edges, output_nodes = tweets_retweets.process(
        final_tweets_classes=input_final_tweets_classes, final_accounts=input_final_tweets_accounts
    )
    pd.testing.assert_frame_equal(edges, output_edges)
    pd.testing.assert_frame_equal(nodes, output_nodes)
