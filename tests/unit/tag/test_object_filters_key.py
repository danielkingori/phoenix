"""Test get_key objects."""
import pandas as pd
import pytest

from phoenix.tag import object_filters
from phoenix.tag.data_pull import constants


@pytest.fixture
def tweets_input_df():
    """Tweets input dataframe."""
    t_c = constants.OBJECT_TYPE_TWEET
    return pd.DataFrame(
        {
            "object_id": [1, 2, 3, 4, 5],
            "object_type": [t_c, t_c, t_c, t_c, "not"],
            "has_key_feature": [True, True, False, False, True],
            "is_retweet": [True, False, True, False, False],
        }
    )


@pytest.fixture
def key_tweets_df():
    """Key tweets."""
    t_c = constants.OBJECT_TYPE_TWEET
    return pd.DataFrame(
        {
            "object_id": [2],
            "object_type": [t_c],
            "has_key_feature": [True],
            "is_retweet": [False],
        },
        index=pd.Int64Index([1], dtype="int64"),
    )


@pytest.fixture
def facebook_posts_input_df():
    """Facebook posts input dataframe."""
    t_c = constants.OBJECT_TYPE_FACEBOOK_POST
    return pd.DataFrame(
        {
            "object_id": [1, 2, 3, 4, 5],
            "object_type": [t_c, t_c, t_c, t_c, "not"],
            "has_key_feature": [True, True, True, False, True],
            "is_retweet": [True, False, False, True, True],
        }
    )


@pytest.fixture
def key_facebook_posts_df():
    """Key facebook posts."""
    t_c = constants.OBJECT_TYPE_FACEBOOK_POST
    return pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "object_type": [t_c] * 3,
            "has_key_feature": [True] * 3,
            "is_retweet": [True, False, False],
        }
    )


def test_key_tweets(tweets_input_df, key_tweets_df):
    """Test filter of tag data for key_tweets."""
    result = object_filters.get_key_tweets(tweets_input_df)
    pd.testing.assert_frame_equal(result, key_tweets_df)


def test_key_facebook_posts(facebook_posts_input_df, key_facebook_posts_df):
    """Test filter of tag data for key_facebook_posts."""
    result = object_filters.get_key_facebook_posts(facebook_posts_input_df)
    pd.testing.assert_frame_equal(result, key_facebook_posts_df)


def test_get_all_key_objects(
    tweets_input_df, facebook_posts_input_df, key_tweets_df, key_facebook_posts_df
):
    """Test get all key objects ."""
    df = pd.concat([tweets_input_df, facebook_posts_input_df])
    result = object_filters.get_all_key_objects(df)
    e_result = pd.concat([key_tweets_df, key_facebook_posts_df])
    pd.testing.assert_frame_equal(result, e_result)