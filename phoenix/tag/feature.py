"""Normalise module."""
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


def get_features_to_label(exploded_features_df) -> pd.DataFrame:
    """Get the features to label."""
    df = exploded_features_df.copy()
    df = df[["features", "features_count", "object_id"]]
    all_feature_count = df.groupby("features").agg({"features_count": "sum", "object_id": pd.Series.nunique})
    all_feature_count = all_feature_count.rename(columns={"object_id": "number_of_items"})
    all_feature_count.sort_values(by="features_count", inplace=True, ascending=False)
    ten_percent = round(all_feature_count.shape[0] * 0.1)
    return all_feature_count[:ten_percent]
