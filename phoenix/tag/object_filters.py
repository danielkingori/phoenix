"""Filters for tagging dataframe.

Dataframes that can be filtered are:
    objects_df: see docs/schemas/objects_df.md
"""
import pandas as pd

from phoenix.tag.data_pull import constants


def tweets(df):
    """Get tweets from dataframe."""
    return df["object_type"] == constants.OBJECT_TYPE_TWEET


def facebook_posts(df):
    """Get facebook_posts from dataframe."""
    return df["object_type"] == constants.OBJECT_TYPE_FACEBOOK_POST


def facebook_comments(df):
    """Get facebook_comments from dataframe."""
    return df["object_type"] == constants.OBJECT_TYPE_FACEBOOK_COMMENT


def not_retweet(df):
    """Get none retweet."""
    return ~df["is_retweet"]


def get_relevant_facebook_posts(df):
    """Get relevant facebook posts."""
    return df[(facebook_posts(df)) & (is_relevant_object(df))]


def get_relevant_facebook_comments(df):
    """Get relevant facebook comments."""
    return df[(facebook_comments(df)) & (is_relevant_object(df))]


def get_relevant_tweets(df):
    """Get relevant tweets."""
    return df[(tweets(df)) & (is_relevant_object(df)) & (not_retweet(df))]


def get_all_relevant_objects(df):
    """Get all relevant objects.

    This is used for determining the `has_topics`.
    """
    return pd.concat(
        [
            get_relevant_tweets(df),
            get_relevant_facebook_posts(df),
            get_relevant_facebook_comments(df),
        ]
    )


def is_relevant_object(df):
    """Is relevant object."""
    return df["has_topics"].isin([True])


def get_relevant_objects(df):
    """All relevant objects.

    This uses the `has_topics` relevant and is for
    finalised object lists.
    """
    return df[is_relevant_object(df)]


def export(df, export_type):
    """Filter dataframe for exporting."""
    if "tweets" == export_type:
        return df[(tweets(df))]

    if "facebook_posts" == export_type:
        return df[(facebook_posts(df))]

    if "facebook_comments" == export_type:
        return df[(facebook_comments(df))]

    if "relevant_facebook_posts" == export_type:
        return get_relevant_facebook_posts(df)

    if "relevant_facebook_comments" == export_type:
        return get_relevant_facebook_comments(df)

    if "relevant_tweets" == export_type:
        return get_relevant_tweets(df)

    if "relevant_objects" == export_type:
        return get_relevant_objects(df)

    raise ValueError(f"Export Type not supported: {export_type}")
