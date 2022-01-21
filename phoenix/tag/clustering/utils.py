"""LDA utils."""
from typing import List, Optional

import pandas as pd

from phoenix.common import artifacts


OBJECTS_DF_COL = ["object_id", "clean_text"]


def apply_grouping_to_objects(
    grouping_type: str,
    object_df: pd.DataFrame,
    topic_df_url: Optional[str] = None,
    exclude_groupings: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Apply the grouping to the objects dataframe so that the we can create an LDA per group later.

    Args:
        grouping_type (str): type of grouping, currently supported "topic"
        object_df (pd.DataFrame): objects dataframe. See docs/schemas/objects.md
        topic_df_url (str): URL of the topic dataframe needed when grouping_type is "topic"
        exclude_groupings (List[str]): List of grouping to exclude from the groupings.

    """
    if grouping_type == "topic":
        if not topic_df_url:
            raise ValueError("topic_df_url is needed for grouping_type topic")
        return apply_grouping_to_objects_topics(object_df, topic_df_url, exclude_groupings)

    return object_df[OBJECTS_DF_COL]


def apply_grouping_to_objects_topics(
    object_df: pd.DataFrame, topic_df_url: str, exclude_groupings: Optional[List[str]]
) -> pd.DataFrame:
    """Apply grouping to objects for topics."""
    topic_df = artifacts.dataframes.get(topic_df_url).dataframe
    object_df = topic_df.merge(object_df[OBJECTS_DF_COL], on="object_id")

    if not exclude_groupings:
        exclude_groupings = []

    if len(exclude_groupings) > 0:
        object_df = object_df[~object_df["topic"].isin(exclude_groupings)]
    return object_df
