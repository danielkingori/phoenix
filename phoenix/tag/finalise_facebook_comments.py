"""Finalise facebook posts, facebook comments and tweets.

This will join objects and topic data frames to respective data source dataframe.
"""
from typing import List

import pandas as pd


FACEBOOK_COMMENT_INHERITABLE_COLUMNS = [
    "classes",
    "has_classes",
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

FACEBOOK_COMMENT_TOPICS_INHERITABLE_COLUMNS = [
    "class",
    "has_class",
    "topic",
    "has_topic",
] + FACEBOOK_COMMENT_INHERITABLE_COLUMNS


FACEBOOK_COMMENT_ACCOUNT_CLASSES_INHERITABLE_COLUMNS = [
    "account_label",
]


def inherited_columns_for_facebook_comments(
    posts_topics_df: pd.DataFrame,
) -> List[str]:
    """Get the inherited_columns for the facebook comments.

    Args:
        posts_topics_df (pd.DataFrame): facebook posts topics dataframe to get the
            inherited columns from.
    """
    return get_inheritable_column_names(posts_topics_df, FACEBOOK_COMMENT_INHERITABLE_COLUMNS)


def inherited_columns_for_facebook_comments_topics(
    posts_topics_df: pd.DataFrame,
) -> List[str]:
    """Get the inherited_columns for the facebook comments.

    Args:
        posts_topics_df (pd.DataFrame): facebook posts topics dataframe to get the
            inherited columns from.
    """
    return get_inheritable_column_names(
        posts_topics_df, FACEBOOK_COMMENT_TOPICS_INHERITABLE_COLUMNS
    )


def inherited_columns_for_facebook_comments_account_classes(
    posts_accounts_objects_df: pd.DataFrame,
) -> List[str]:
    """Get the inherited_columns for the facebook comments account classes.

    Args:
        posts_accounts_objects_df (pd.DataFrame): facebook posts objects account
            classes dataframe to get the inherited columns from.
    """
    return get_inheritable_column_names(
        posts_accounts_objects_df, FACEBOOK_COMMENT_ACCOUNT_CLASSES_INHERITABLE_COLUMNS
    )


def get_inheritable_column_names(
    df: pd.DataFrame,
    inheritable_columns: List[str],
) -> List[str]:
    """Get the inheritable columns that the dataframe contains.

    Args:
        df (pd.DataFrame): dataframe to get the inherited columns from.
        inheritable_columns (List[str]): list of columns that can be inherited.

    Returns:
        (List[str]): List of columns that can be inherited from the facebook posts topics
    """
    avaliable_columns = set(df.columns)
    to_inherit = set(inheritable_columns)
    return list(avaliable_columns.intersection(to_inherit))


def inherit_from_facebook_posts_topics_df(
    posts_topics_df: pd.DataFrame,
    comments_df: pd.DataFrame,
    inherited_columns: List[str],
    inherit_every_row_per_id: bool = False,
) -> pd.DataFrame:
    """Joins comments to their parent posts and inherits tensions and topic(s) from parents.

    Args:
        posts_topics_df (pd.DataFrame): fb_posts dataframe with topics
            and other columns to inherit.
        comments_df (pd.DataFrame): base comments which inherit certain
            characteristics from parents.
        inherited_columns: columns that will be inherited from the posts_topics_df.
        inherit_every_row_per_id (bool): If there are multiple rows
            per post_id (eg two `topic`s for a post),
            should we inherit each of those rows.
        rename_topic_to_class (boolean): if the `topic` column is rename to `class`.
            Default is False

    Returns:
        Dataframe with the comments inheriting from the posts topics.
    """
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
