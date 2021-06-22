"""Single feature match for topic analysis."""
import pandas as pd
import tentaclio

from phoenix.common import artifacts


DEFAULT_RAW_TOPIC_CONFIG = "single_feature_match_topic_config.csv"


def get_topics(topic_config, features_df) -> pd.DataFrame:
    """Get the topics.

    Return:
    pd.DataFrame:
        Index: object_id
        topic: string
        matched_features: array<string>
    """
    features_indexed_df = features_df.set_index("object_id")
    topic_config_i = topic_config.set_index("features")
    topics_df = features_indexed_df.join(topic_config_i, on="features")
    topics_df = topics_df[~topics_df["topic"].isnull()]
    topics_df = (
        topics_df.groupby(["object_id", "topic"])
        .agg({"features": list})
        .rename(columns={"features": "matched_features"})
    )
    return topics_df.reset_index()


def get_topic_config(config_url=None) -> pd.DataFrame:
    """Get topic config dataframe."""
    df = _get_raw_topic_config(config_url)
    df["topic"] = df["topic"].str.lower()
    df = df[~df["topic"].isin(["no tag", "other"])].dropna()
    df["topic_list"] = df["topic"].str.split(",")
    df_ex = (
        df.explode("topic_list", ignore_index=True)
        .drop("topic", axis=1)
        .rename(columns={"topic_list": "topic"})
    )
    df_ex["topic"] = df_ex["topic"].str.strip()
    return df_ex[["features", "topic"]]


def _get_raw_topic_config(config_url=None) -> pd.DataFrame:
    """Get the raw topic_config."""
    if not config_url:
        config_url = f"{artifacts.urls.get_static_config()}{DEFAULT_RAW_TOPIC_CONFIG}"

    with tentaclio.open(config_url, "r") as fb:
        df = pd.read_csv(fb)
    return df
