"""Filters for objects."""
import pandas as pd

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


def get_key_facebook_posts(df):
    """Filter key facebook posts."""
    return df[(facebook_posts(df)) & (key_feature(df))]


def get_key_tweets(df):
    """Key tweets."""
    return df[(tweets(df)) & (key_feature(df)) & (not_retweet(df))]


def get_all_key_objects(df):
    """All key objects.

    This is used for determining the `is_key_object`.
    """
    return pd.concat([get_key_tweets(df), get_key_facebook_posts(df)])


def is_key_object(df):
    """Is key object."""
    return df["is_key_object"].isin([True])


def get_key_objects(df):
    """All key objects.

    This uses the `is_key_object` key and is for
    finalised object lists.
    """
    return df[is_key_object(df)]


def export(df, export_type):
    """Filter dataframe for exporting."""
    if "tweets" == export_type:
        return df[(tweets(df))]

    if "facebook_posts" == export_type:
        return df[(facebook_posts(df))]

    if "key_facebook_posts" == export_type:
        return df[(facebook_posts(df)) & (is_key_object(df))]

    if "key_tweets" == export_type:
        return df[(tweets(df)) & (is_key_object(df))]

    if "key_objects" == export_type:
        return df[(is_key_object(df))]

    raise ValueError(f"Export Type not supported: {export_type}")
