"""Normalise module."""
import pandas as pd

from phoenix.common import artifacts
from phoenix.tag import text_features_analyser


def features(given_df: pd.DataFrame, message_key: str = "clean_message") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[[message_key, "language"]], message_key)
    return df


def explode_features(given_df: pd.DataFrame):
    """Explode dataframe by the features_index."""
    df = given_df.copy()
    df["features_count"] = text_features_analyser.ngram_count(df[["features"]])
    df["features_index"] = text_features_analyser.features_index(
        df[["features_count"]]
    )
    ex_df = df.explode("features_index")
    ex_df["features"] = ex_df["features_index"].str[1]
    ex_df["features_count"] = ex_df["features_index"].str[2].fillna(0).astype(int)
    ex_df["index"] = ex_df.index
    return ex_df.drop(
        [
            "features_index",
        ],
        axis=1,
    )


def key_features(given_df, features_key: str = "features") -> pd.Series:
    """Return key features."""
    df = given_df.copy()
    interesting_features = pd.read_csv(
        f"{artifacts.urls.get_static_config()}/interesting_features.csv"
    )
    return df[features_key].isin(interesting_features["interesting_features"])


def get_key_posts(exploded_features_df) -> pd.DataFrame:
    """Get the posts with key features."""
    exploded_features_df["has_key_feature"] = key_features(exploded_features_df[["features"]])
    posts = (
        exploded_features_df.groupby("index")
        .first()
        .drop(columns=["features", "features_count"])
    )
    return posts[posts["has_key_feature"].isin([True])]
