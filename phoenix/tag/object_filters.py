"""Filters for objects."""
from phoenix.tag.data_pull import constants


def tweets(df):
    """Get tweets from dataframe."""
    return df["object_type"] == constants.OBJECT_TYPE_TWEET


def facebook_posts(df):
    """Get facebook_posts from dataframe."""
    return df["object_type"] == constants.OBJECT_TYPE_FACEBOOK_POST


def key_feature(df):
    """Get key features from dataframe."""
    return df["has_key_feature"].isin([True])


def not_retweet(df):
    """Not retweet."""
    return ~df["is_retweet"]
