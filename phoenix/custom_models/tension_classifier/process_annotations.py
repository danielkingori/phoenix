"""Process annotations of tensions."""
import logging

import pandas as pd

logger = logging.getLogger()


def process_annotations(df: pd.DataFrame) -> pd.DataFrame:
    """Process the raw annotations."""
    df = update_column_names(df)
    df = clean_features(df)

    return df


def update_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Update the column names in dataframe.

    The annotation sheet is multi-indexed with many Unnamed cols, thus it's easiest to rename it
    instead of running a mapping from multi-index to normalized column names.
    """
    assert len(df.columns) == 35

    column_names_annotations = [
        "sheet_index",
        "object_id",
        "object_type",
        "post_url",
        "account_url",
        "topics",
        "matched_features",
        "text",
        "added_topics_without_features",
        "added_topics_analyst_with_features",
        "added_topic_features",
        "economic_labour_tensions",
        "economic_labour_tensions_features",
        "economic_labour_tensions_direction",
        "sectarian_tensions",
        "sectarian_tensions_features",
        "sectarian_tensions_direction",
        "environmental_tensions",
        "environmental_tensions_features",
        "environmental_tensions_direction",
        "political_tensions",
        "political_tensions_features",
        "political_tensions_direction",
        "service_related_tensions",
        "service_related_tensions_features",
        "service_related_tensions_direction",
        "community_insecurity_tensions",
        "community_insecurity_tensions_features",
        "community_insecurity_tensions_direction",
        "geopolitics_tensions",
        "geopolitics_tensions_features",
        "geopolitics_tensions_direction",
        "intercommunity_relations_tensions",
        "intercommunity_relations_tensions_features",
        "intercommunity_relations_tensions_direction",
    ]

    df.columns = column_names_annotations
    df = df.where(df.isnull(), df.astype(str))

    return df


def clean_features(df: pd.DataFrame) -> pd.DataFrame:
    """Clean any features from having symbols."""
    features_cols = [col for col in df.columns if "features" in col]

    for features_col in features_cols:
        if df[features_col].dtype == object:
            df[features_col] = df[features_col].str.replace(r"\n", "", regex=True)

    return df
