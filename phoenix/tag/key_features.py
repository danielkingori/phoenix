"""Key features tagging."""
import pandas as pd

from phoenix.common import artifacts


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
        .drop(columns=["features", "features_1_count", "features_count"])
    )
    return posts[posts["has_key_feature"].isin([True])]
