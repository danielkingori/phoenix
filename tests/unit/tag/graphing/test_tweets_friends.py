"""Tests for the twitter friends graph."""

import pandas as pd
import pytest

from phoenix.tag.graphing import tweets_friends


@pytest.fixture
def input_twitter_friends() -> pd.DataFrame:
    """Input dataframe of twitter_fiends."""
    return pd.DataFrame(
        {
            "user_screen_name": ["u_1", "u_1", "u_2", "u_2", "u_3"],
            "followed_user_id": ["u_2", "u_3", "u_4", "u_1", "u_2"],
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
            "user_screen_name": ["u_1", "u_1", "u_2", "u_2", "u_3"],
            "followed_user_screen_name": ["u_2", "u_3", "u_4", "u_1", "u_2"],
        }
    )


@pytest.fixture
def nodes() -> pd.DataFrame:
    """Expected output nodes dataframe."""
    return pd.DataFrame(
        {
            "user_screen_name": ["u_1", "u_2", "u_3", "u_4"],
            "account_label": ["acc_label_1, acc_label_2", "acc_label_2", "", "acc_label_1"],
        }
    )


def test_process(input_twitter_friends, input_final_tweets_accounts, edges, nodes):
    """Test processing inputs to edges and nodes."""
    output_edges, output_nodes = tweets_friends.process(
        twitter_friends=input_twitter_friends, final_accounts=input_final_tweets_accounts
    )
    pd.testing.assert_frame_equal(edges, output_edges)
    pd.testing.assert_frame_equal(nodes, output_nodes)
