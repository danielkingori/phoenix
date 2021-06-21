"""Test Object filters."""
import pandas as pd

from phoenix.tag import object_filters
from phoenix.tag.data_pull import constants


def test_tweets():
    """Test tweets filter."""
    df = pd.DataFrame({"object_id": [1, 2], "object_type": [constants.OBJECT_TYPE_TWEET, "non"]})
    result = df[object_filters.tweets(df)]
    pd.testing.assert_frame_equal(
        result, pd.DataFrame({"object_id": [1], "object_type": [constants.OBJECT_TYPE_TWEET]})
    )


def test_facebook_posts():
    """Test facebook_posts filter."""
    df = pd.DataFrame(
        {"object_id": [1, 2], "object_type": [constants.OBJECT_TYPE_FACEBOOK_POST, "non"]}
    )
    result = df[object_filters.facebook_posts(df)]
    pd.testing.assert_frame_equal(
        result,
        pd.DataFrame({"object_id": [1], "object_type": [constants.OBJECT_TYPE_FACEBOOK_POST]}),
    )


def test_key_feature():
    """Test key_feature filter."""
    df = pd.DataFrame({"object_id": [1, 2], "has_key_feature": [True, False]})
    result = df[object_filters.key_feature(df)]
    pd.testing.assert_frame_equal(
        result, pd.DataFrame({"object_id": [1], "has_key_feature": [True]})
    )


def test_not_retweet():
    """Test not a retweet filter."""
    df = pd.DataFrame({"object_id": [1, 2], "is_retweet": [False, True]})
    result = df[object_filters.not_retweet(df)]
    pd.testing.assert_frame_equal(result, pd.DataFrame({"object_id": [1], "is_retweet": [False]}))
