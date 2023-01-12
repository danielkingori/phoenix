"""Test get_relevant objects."""
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
            "has_topics": [True, True, False, False, True],
            "is_retweet": [True, False, True, False, False],
        }
    )


@pytest.fixture
def relevant_tweets_df():
    """Key tweets."""
    t_c = constants.OBJECT_TYPE_TWEET
    return pd.DataFrame(
        {
            "object_id": [2],
            "object_type": [t_c],
            "has_topics": [True],
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
            "has_topics": [True, True, True, False, True],
            "is_retweet": [True, False, False, True, True],
        }
    )


@pytest.fixture
def relevant_facebook_posts_df():
    """Key facebook posts."""
    t_c = constants.OBJECT_TYPE_FACEBOOK_POST
    return pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "object_type": [t_c] * 3,
            "has_topics": [True] * 3,
            "is_retweet": [True, False, False],
        }
    )


def test_relevant_tweets(tweets_input_df, relevant_tweets_df):
    """Test filter of tag data for relevant_tweets."""
    result = object_filters.get_relevant_tweets(tweets_input_df)
    pd.testing.assert_frame_equal(result, relevant_tweets_df)


def test_relevant_facebook_posts(facebook_posts_input_df, relevant_facebook_posts_df):
    """Test filter of tag data for relevant_facebook_posts."""
    result = object_filters.get_relevant_facebook_posts(facebook_posts_input_df)
    pd.testing.assert_frame_equal(result, relevant_facebook_posts_df)


def test_get_all_relevant_objects(
    tweets_input_df, facebook_posts_input_df, relevant_tweets_df, relevant_facebook_posts_df
):
    """Test get all relevant objects ."""
    df = pd.concat([tweets_input_df, facebook_posts_input_df])
    result = object_filters.get_all_relevant_objects(df)
    e_result = pd.concat([relevant_tweets_df, relevant_facebook_posts_df])
    pd.testing.assert_frame_equal(result, e_result)
