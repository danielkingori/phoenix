"""Functionality for building the features dataframe.

Final dataframe schema: docs/schemas/features.md

Please update if it is changed.
"""
import pandas as pd

from phoenix.common import artifacts
from phoenix.tag import text_features_analyser


def features(given_df: pd.DataFrame, text_key: str = "clean_text") -> pd.DataFrame:
    """Append features column.

    Arguments:
        given_df: A dataframe with text column and language column.
        text_key: The column name of the text. Default "clean_text".

    Returns
        given_df copy with "features" column.
    """
    df = given_df.copy()
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[[text_key, "language"]], text_key)
    return df


def explode_features(given_df: pd.DataFrame):
    """Explode dataframe by the features.

    Creating a row for each feature in the list.
    This will repeat all the data across a features list.

    Arguments
        given_df: A dataframe with a "features" column and "object_id"

    Returns:
        pd.DataFrame with dtypes:
            object_id:  (index) string,
            ... (other columns added)
            features: string,
            features_count: int
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


def key_features(given_df: pd.DataFrame, features_key: str = "features") -> pd.Series:
    """Get the key features.

    Arguments:
        given_df: a dataframe containing "features" as a single string.

    Returns:
        Subset of given_df that has interesting_features.
    """
    df = given_df.copy()
    interesting_features = get_interesting_features()
    return df[features_key].isin(interesting_features["interesting_features"])


def get_interesting_features() -> pd.DataFrame:
    """Get interesting features configuration."""
    return pd.read_csv(f"{artifacts.urls.get_static_config()}/interesting_features.csv")


def get_features_to_label(exploded_features_df) -> pd.DataFrame:
    """Get the features to label.

    The idea is that this will return the most common features grouped by the frequency.
    This dataframe can then be used to create new feature to topic mappings.

    Arguments:
        exploded_features_df: see docs/schemas/features.md

    Returns:
        pd.DataFrame with dtypes:
            features: string,
            features_count: int
            number_of_objects: int
    """
    df = exploded_features_df.copy()
    df = df[["features", "features_count", "object_id"]]
    all_feature_count = df.groupby("features").agg(
        {"features_count": "sum", "object_id": pd.Series.nunique}
    )
    all_feature_count = all_feature_count.rename(columns={"object_id": "number_of_objects"})
    all_feature_count.sort_values(by="features_count", inplace=True, ascending=False)
    ten_percent = round(all_feature_count.shape[0] * 0.1)
    return all_feature_count[:ten_percent]