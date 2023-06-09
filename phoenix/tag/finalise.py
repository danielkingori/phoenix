"""Finalise facebook posts, facebook comments and tweets.

This will join objects and topic data frames to respective data source dataframe.
"""
from typing import List, Optional

import pandas as pd

from phoenix.common import artifacts


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

PARTITION_COLUMNS_TO_DROP: List[str] = [
    # Remove of the columns to drop
]


def for_object_type(
    object_type: str,
    df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
    rename_topic_to_class: bool = False,
) -> pd.DataFrame:
    """Finalise the dataframe for an object type."""
    if object_type in ["youtube_videos", "youtube_comments"]:
        df["object_id"] = df["id"].astype(str)
        df = df.set_index("object_id")
        df = df.drop(PARTITION_COLUMNS_TO_DROP, axis=1)
        return join_to_objects_and_language_sentiment(
            df,
            objects_df,
            language_sentiment_objects_df,
            rename_topic_to_class,
        )

    raise RuntimeError(f"Object Type: {object_type}. Not supported.")


def topics_for_object_type(
    object_type: str,
    df: pd.DataFrame,
    topics_df: pd.DataFrame,
    rename_topic_to_class: bool = False,
) -> pd.DataFrame:
    """Finalise the topics dataframe for an object type."""
    if object_type in ["youtube_videos", "youtube_comments"]:
        df = df.copy()
        df["object_id"] = df["id"].astype(str)
        df = df.set_index("object_id")
        return join_to_topics(
            df,
            topics_df,
            rename_topic_to_class,
        )

    raise RuntimeError(f"Object Type: {object_type}. Not supported.")


def join_to_objects_and_language_sentiment(
    df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
    rename_topic_to_class: Optional[bool] = False,
):
    """Generalised join of objects_df and language_sentiment_objects_df to a final dataframe."""
    if objects_df is not None and rename_topic_to_class:
        objects_df = topic_to_class(objects_df)

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
    rename_topic_to_class: bool = False,
):
    """Join the objects_df to the facebook_posts."""
    facebook_posts_df["object_id"] = facebook_posts_df["phoenix_post_id"].astype(str)
    facebook_posts_df = facebook_posts_df.set_index("object_id")
    facebook_posts_df = facebook_posts_df.drop(PARTITION_COLUMNS_TO_DROP, axis=1)
    return join_to_objects_and_language_sentiment(
        facebook_posts_df,
        objects_df,
        language_sentiment_objects_df,
        rename_topic_to_class,
    )


def join_objects_to_facebook_comments(
    facebook_comments_df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
    rename_topic_to_class: Optional[bool] = False,
):
    """Join the objects to the facebook_comments."""
    facebook_comments_df["object_id"] = facebook_comments_df["id"].astype(str)
    facebook_comments_df = facebook_comments_df.set_index("object_id")
    facebook_comments_df = facebook_comments_df.drop(PARTITION_COLUMNS_TO_DROP, axis=1)
    return join_to_objects_and_language_sentiment(
        facebook_comments_df, objects_df, language_sentiment_objects_df, rename_topic_to_class
    )


def join_objects_to_tweets(
    tweets_df: pd.DataFrame,
    objects_df: Optional[pd.DataFrame] = None,
    language_sentiment_objects_df: Optional[pd.DataFrame] = None,
    rename_topic_to_class: Optional[bool] = False,
):
    """Join the objects_df to the tweets_df."""
    tweets_df["object_id"] = tweets_df["id_str"].astype(str)
    tweets_df = tweets_df.set_index("object_id")
    tweets_df = tweets_df.drop(PARTITION_COLUMNS_TO_DROP, axis=1)
    if objects_df is not None:
        objects_df = objects_df.drop(columns=["retweeted", "text", "language_from_api"])
    return join_to_objects_and_language_sentiment(
        tweets_df,
        objects_df,
        language_sentiment_objects_df,
        rename_topic_to_class,
    )


def topic_to_class(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the topic columns duplicating to class columns.

    Args:
        df (dataframe): with possible "topic" or "topics"

    Returns:
        DataFrame with `has_class` and `class` or `has_classes` and `classes`
    """
    if "topic" in df.columns:
        df[["class", "has_class"]] = df[["topic", "has_topic"]]

    if "topics" in df.columns:
        df[["classes", "has_classes"]] = df[["topics", "has_topics"]]
    return df


def join_to_topics(df: pd.DataFrame, topics_df: pd.DataFrame, rename_topic_to_class: bool = False):
    """Join a dataframe with `object_id` to topics_df.

    Arg:
        df: a dataframe with "object_id" as the index
        topics_df: topics dataframe schema: docs/schemas/topics.md
        rename_topic_to_class (boolean): if the `topic` column is renamed to `class`.
            Default is False

    Returns:
        Add dataframe with each object in the df repeated for each topic.
    """
    topics_df = topics_df[TOPICS_COLUMNS]
    if rename_topic_to_class:
        topics_df = topic_to_class(topics_df)
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(df, how="right")
    return result_df.reset_index()


def join_topics_to_facebook_posts(
    topics_df: pd.DataFrame,
    facebook_posts_df: pd.DataFrame,
    rename_topic_to_class: bool = False,
) -> pd.DataFrame:
    """Join the topics to the facebook_posts.

    Args:
        topics_df: topics dataframe schema: docs/schemas/topics.md
        facebook_posts_df: facebook posts dataframe.
        rename_topic_to_class (boolean): if the `topic` column is renamed to `class`.
            Default is False

    Returns:
        A dataframe that is the cross product of topic and facebook posts:
        a new row for each topic that a facebook posts has
    """
    facebook_posts_df = facebook_posts_df.copy()
    facebook_posts_df["object_id"] = facebook_posts_df["phoenix_post_id"].astype(str)
    facebook_posts_df = facebook_posts_df.set_index("object_id")
    return join_to_topics(facebook_posts_df, topics_df, rename_topic_to_class)


def join_topics_to_tweets(
    topics_df: pd.DataFrame,
    tweets_df: pd.DataFrame,
    rename_topic_to_class: bool = False,
) -> pd.DataFrame:
    """Join the topics to the tweets.

    Args:
        topics_df: topics dataframe schema: docs/schemas/topics.md
        tweets_df: tweets dataframe.
        rename_topic_to_class (boolean): if the `topic` column is renamed to `class`.
            Default is False

    Returns:
        A dataframe that is the cross product of topic and tweets:
        a new row for each topic that a tweets
    """
    tweets_df = tweets_df.copy()
    tweets_df["object_id"] = tweets_df["id_str"].astype(str)
    tweets_df = tweets_df.set_index("object_id")
    tweets_df = tweets_df.drop(columns=["retweeted"])
    return join_to_topics(tweets_df, topics_df, rename_topic_to_class)


def join_topics_to_facebook_comments(
    topics_df: pd.DataFrame,
    facebook_comments_df: pd.DataFrame,
    rename_topic_to_class: bool = False,
) -> pd.DataFrame:
    """Join the topics to the facebook comments.

    Args:
        topics_df: topics dataframe schema: docs/schemas/topics.md
        tweets_df: facebook comments dataframe.
        rename_topic_to_class (boolean): if the `topic` column is renamed to `class`.
            Default is False

    Returns:
        A dataframe that is the cross product of topic and facebook_comments:
        a new row for each topic that a facebook comments
    """
    facebook_comments_df = facebook_comments_df.copy()
    facebook_comments_df["object_id"] = facebook_comments_df["id"].astype(str)
    facebook_comments_df = facebook_comments_df.set_index("object_id")
    return join_to_topics(facebook_comments_df, topics_df, rename_topic_to_class)


def persist_partitions(artifact_url: str, df: pd.DataFrame):
    """Persist the final datasets as partitioned dataset by `year_filter`, `month_filter`.

    This is also configured to overwrite the partitions dynamically.
    Meaning all the partitions that are included in the given dataframe
    will be overwritten. Partitions not included in the dataset will be
    left.

    For an further explanation of this see:
    tests/integration/common/artifacts/test_dask_dataframes_local.py
    """
    to_parquet_params = {
        "engine": "pyarrow",
        "partition_on": ["year_filter", "month_filter"],
        "overwrite": False,
        "write_metadata_file": False,
    }
    ddf = artifacts.utils.pandas_to_dask(df)
    artifacts.dask_dataframes.persist(artifact_url, ddf, to_parquet_params)
