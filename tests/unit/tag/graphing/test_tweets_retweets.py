"""Tests for the twitter retweets graph."""

import pandas as pd
import pytest


@pytest.fixture
def input_final_tweets() -> pd.DataFrame:
    """Input dataframe of tweets."""
    return pd.DataFrame(
        {
            "user_screen_name": ["u_1", "u_2", "u_1", "u_3", "u_3"],
            "retweeted_user_screen_name": [None, "u_1", "u_2", "u_1", "u_1"],
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
            "retweeted_user_screen_name": ["u_1", "u_2", "u_1"],
            "tweeting_user_screen_name": ["u_2", "u_1", "u_3"],
            "times_retweeted": [1, 1, 2],
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
