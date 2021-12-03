"""Finalise facebook posts, facebook comments and tweets.

This will join objects and topic data frames to respective data source dataframe.
"""
from typing import List, Optional

import pandas as pd


LANGUAGE_SENTIMENT_COLUMNS = [
    "language_sentiment",
    "language_sentiment_score_mixed",
    "language_sentiment_score_neutral",
    "language_sentiment_score_negative",
    "language_sentiment_score_positive",
]

TOPICS_COLUMNS = [
    "object_id",
    "topic",
    "matched_features",
    "has_topic",
]

COMMENT_INHERITED_COLUMNS = [
    "topics",
    "has_topics",
    "is_economic_labour_tension",
    "is_political_tension",
    "is_service_related_tension",
    "is_community_insecurity_tension",
    "is_sectarian_tension",
    "is_environmental_tension",
    "is_geopolitics_tension",
    "is_intercommunity_relations_tension",
    "has_tension",
]

PARTITION_COLUMNS_TO_DROP = [
    "year_filter",
    "month_filter",
]


def join_to_objects_and_language_sentiment(
    df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
):
    """Generalised join of objects_df and language_sentiment_objects_df to a final dataframe."""
    if objects_df is None and language_sentiment_objects_df is not None:
        language_sentiment_objects_df = language_sentiment_objects_df.set_index("object_id")
        return df.join(
            language_sentiment_objects_df[LANGUAGE_SENTIMENT_COLUMNS], rsuffix="_objects"
        )

    if objects_df is not None:
        objects_df = objects_df.set_index("object_id")

    if objects_df is not None and language_sentiment_objects_df is not None:
        language_sentiment_objects_df = language_sentiment_objects_df.set_index("object_id")
        objects_df = objects_df.join(language_sentiment_objects_df[LANGUAGE_SENTIMENT_COLUMNS])

    if objects_df is not None:
        return df.join(objects_df, rsuffix="_objects_df")

    return df


def join_objects_to_facebook_posts(
    facebook_posts_df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
):
    """Join the objects_df to the facebook_posts."""
    facebook_posts_df["object_id"] = facebook_posts_df["phoenix_post_id"].astype(str)
    facebook_posts_df = facebook_posts_df.set_index("object_id")
    facebook_posts_df = facebook_posts_df.drop(PARTITION_COLUMNS_TO_DROP, axis=1)
    return join_to_objects_and_language_sentiment(
        facebook_posts_df, objects_df, language_sentiment_objects_df
    )


def join_objects_to_facebook_comments(objects, language_sentiment_objects, facebook_comments):
    """Join the objects to the facebook_comments."""
    objects = objects.set_index("object_id")
    language_sentiment_objects = language_sentiment_objects.set_index("object_id")
    objects = objects.join(language_sentiment_objects[LANGUAGE_SENTIMENT_COLUMNS])
    facebook_comments["object_id"] = facebook_comments["id"].astype(str)
    facebook_comments = facebook_comments.set_index("object_id")
    return facebook_comments.join(objects, rsuffix="_objects")


def join_objects_to_tweets(
    tweets_df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
):
    """Join the objects_df to the tweets_df."""
    tweets_df["object_id"] = tweets_df["id_str"].astype(str)
    tweets_df = tweets_df.set_index("object_id")
    tweets_df = tweets_df.drop(PARTITION_COLUMNS_TO_DROP, axis=1)
    if objects_df is not None:
        objects_df = objects_df.drop(columns=["retweeted", "text", "language_from_api"])
    return join_to_objects_and_language_sentiment(
        tweets_df, objects_df, language_sentiment_objects_df
    )


def join_topics_to_facebook_posts(topics, facebook_posts):
    """Join the topics to the facebook_posts."""
    facebook_posts_df = facebook_posts.copy()
    facebook_posts_df["object_id"] = facebook_posts_df["phoenix_post_id"].astype(str)
    facebook_posts_df = facebook_posts_df.set_index("object_id")
    topics_df = topics[TOPICS_COLUMNS]
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(facebook_posts_df, how="right")
    return result_df.reset_index()


def join_topics_to_tweets(topics, tweets):
    """Join the topics to the tweets."""
    tweets_df = tweets.copy()
    tweets_df["object_id"] = tweets_df["id_str"].astype(str)
    tweets_df = tweets_df.set_index("object_id")
    tweets_df = tweets_df.drop(columns=["retweeted"])
    topics_df = topics[TOPICS_COLUMNS]
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(tweets_df, how="right")
    return result_df.reset_index()


def join_topics_to_facebook_comments(topics, facebook_comments):
    """Join the topics to the tweets."""
    facebook_comments_df = facebook_comments.copy()
    facebook_comments_df["object_id"] = facebook_comments_df["id"].astype(str)
    facebook_comments_df = facebook_comments_df.set_index("object_id")
    topics_df = topics[TOPICS_COLUMNS]
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(facebook_comments_df, how="right")
    return result_df.reset_index()


def inherit_facebook_comment_topics_from_facebook_posts_topics_df(
    posts_topics_df: pd.DataFrame,
    comments_df: pd.DataFrame,
    inherit_every_row_per_id: bool = False,
    extra_inherited_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Joins comments to their parent posts and inherits tensions and topic(s) from parents.

    posts_topics_df (pd.DataFrame): fb_posts dataframe with topics and other columns to inherit.
    comments_df (pd.DataFrame) base comments which inherit certain characteristics from parents.
    inherit_every_row_per_id (bool): If there are multiple rows per post_id (eg two `topic`s
        for a post), should we inherit each of those rows.
    extra_inherited_cols (Optional[List[str]]) extra columns to overwrite with posts information.
    """
    if extra_inherited_cols:
        inherited_columns = extra_inherited_cols + COMMENT_INHERITED_COLUMNS
    else:
        inherited_columns = COMMENT_INHERITED_COLUMNS

    for col in inherited_columns:
        if col not in posts_topics_df.columns:
            raise Exception(f"Column {col} not found in posts dataframe.")
    if "post_id" not in comments_df.columns:
        raise Exception("Column 'post_id' not found in comments dataframe.")

    comments_df = comments_df.drop(inherited_columns, axis=1, errors="ignore")

    # Remove any duplicate comment_ids from any other processing step.
    comments_df = comments_df.groupby("id").first().reset_index()

    posts_topics_df = posts_topics_df[["url_post_id"] + inherited_columns]
    posts_topics_df["url_post_id"] = posts_topics_df["url_post_id"].astype(int)

    # only take the last one if you're only interested in the aggregated columns in
    # COMMENT_INHERITED_COLUMNS. If there are multiple rows per post (multiple `topic`s per
    # post) that need to be inherited, turn this flag on.
    if not inherit_every_row_per_id:
        posts_topics_df = posts_topics_df.groupby("url_post_id").last().reset_index()

    comments_df = pd.merge(
        comments_df, posts_topics_df, left_on="post_id", right_on="url_post_id", how="left"
    ).drop("url_post_id", axis=1)

    return comments_df
