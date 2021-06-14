"""Normalise module."""
from typing import Tuple

import pandas as pd

from phoenix.common import artifacts
from phoenix.tag import text_features_analyser


def features(given_df: pd.DataFrame, text_key: str = "clean_text") -> pd.DataFrame:
    """Tag Data.


    Return:
    pd.DataFrame
    object_id, text, clean_text, language, features
    ...                                 , list of features from the clean text
    """
    df = given_df.copy()
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[[text_key, "language"]], text_key)
    return df


def explode_features(given_df: pd.DataFrame):
    """Explode dataframe by the features_index.

    Returns:
    pd.DataFrame:
    object_id, ..., features, features_count
    ...         , string, integer (number of times the feature occurs in the object)
    """
    df = given_df.copy()
    df["features_count"] = text_features_analyser.ngram_count(df[["features"]])
    df["features_index"] = text_features_analyser.features_index(df[["features_count"]])
    # Will create the index on object_id so that it is quicker to groupby
    df = df.set_index("object_id", drop=False)
    ex_df = df.explode("features_index")
    ex_df["features"] = ex_df["features_index"].str[1]
    ex_df["features_count"] = ex_df["features_index"].str[2].fillna(0).astype(int)
    return ex_df.drop(
        [
            "features_index",
        ],
        axis=1,
    )


def key_features(given_df, features_key: str = "features") -> pd.Series:
    """Return key features."""
    df = given_df.copy()
    interesting_features = get_interesting_features()
    return df[features_key].isin(interesting_features["interesting_features"])


def get_interesting_features():
    """Get interesting_features."""
    return pd.read_csv(f"{artifacts.urls.get_static_config()}/interesting_features.csv")


def get_key_items(exploded_features_is_key_df, features_key: str = "features") -> pd.DataFrame:
    """Get the items with key features."""
    df = exploded_features_is_key_df.copy()
    key_items = (
        df[df["is_key_feature"].isin([True])]
        .groupby(level=0)
        .first()
        .drop(columns=["features", "features_count"])
        .rename(columns={"is_key_feature": "has_key_feature"})
    )
    return key_items


def get_feature_items(exploded_features_df, features_key: str = "features") -> pd.DataFrame:
    """Get the items with key features."""
    g = exploded_features_df.groupby(level=0)
    items_features = g.agg({"features": list, "features_count": list})
    return items_features


def finalise(
    all_items, exploded_features_df, features_key: str = "features"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Get the final features and items dataframes."""
    df = exploded_features_df.copy()
    df["is_key_feature"] = key_features(df[[features_key]])
    key_items = get_key_items(df)
    items_features = get_feature_items(df)
    df_has = df.join(key_items[["has_key_feature"]]).fillna(False)
    items = all_items.copy()
    items = items.set_index("object_id", drop=False)
    items = items.join(key_items[["has_key_feature"]]).fillna(False)
    items = items.join(items_features[["features", "features_count"]])
    items = items.reset_index(drop=True)
    key_items_result = items[items["has_key_feature"].isin([True])]
    return items, key_items_result, df_has


def get_features_to_label(exploded_features_df) -> pd.DataFrame:
    """Get the features to label."""
    df = exploded_features_df.copy()
    df = df[["features", "features_count", "object_id"]]
    all_feature_count = df.groupby("features").agg({"features_count": "sum", "object_id": "count"})
    all_feature_count = all_feature_count.rename(columns={"object_id": "number_of_items"})
    all_feature_count.sort_values(by="features_count", inplace=True, ascending=False)
    ten_percent = round(all_feature_count.shape[0] * 0.1)
    return all_feature_count[:ten_percent]
