"""Finalise facebook posts, facebook comments and tweets.

This will join objects and topic data frames to respective data source dataframe.
"""
from typing import List

import pandas as pd


FACEBOOK_COMMENT_INHERITABLE_COLUMNS = [
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
    "topic",
    "has_topic",
] + FACEBOOK_COMMENT_INHERITABLE_COLUMNS


def inherited_columns_for_facebook_comments(
    posts_topics_df: pd.DataFrame,
) -> List[str]:
    """Get the inherited_columns for the facebook comments.

    Args:
        posts_topics_df (pd.DataFrame): facebook posts topics dataframe to get the
            inherited columns from.
    """
    return build_inherited_columns_from_posts_topics_df(
        posts_topics_df, FACEBOOK_COMMENT_INHERITABLE_COLUMNS
    )


def inherited_columns_for_facebook_comments_topics(
    posts_topics_df: pd.DataFrame,
) -> List[str]:
    """Get the inherited_columns for the facebook comments.

    Args:
        posts_topics_df (pd.DataFrame): facebook posts topics dataframe to get the
            inherited columns from.
    """
    return build_inherited_columns_from_posts_topics_df(
        posts_topics_df, FACEBOOK_COMMENT_TOPICS_INHERITABLE_COLUMNS
    )


def build_inherited_columns_from_posts_topics_df(
    posts_topics_df: pd.DataFrame,
    inheritable_columns: List[str],
) -> List[str]:
    """Build the inheritable_columns.

    Args:
        posts_topics_df (pd.DataFrame): facebook posts topics dataframe to get the
            inherited columns from.
        inheritable_columns (List[str]): list of columns that can be inherited.

    Returns:
        (List[str]): List of columns that can be inherited from the facebook posts topics
    """
    avaliable_columns = set(posts_topics_df.columns)
    to_inherit = set(inheritable_columns)
    return list(avaliable_columns.intersection(to_inherit))
