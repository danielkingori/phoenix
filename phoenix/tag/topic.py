"""Topic tagging."""
import pandas as pd
import tentaclio

from phoenix.common import artifacts


DEFAULT_RAW_TOPIC_CONFIG = "raw_topic_config.csv"


def get_topics(topic_config, features_df) -> pd.DataFrame:
    """Get the topics.

    Return:
    pd.DataFrame:
        Index: object_id
        topic: string
    """
    dfs = []
    features_indexed_df = features_df.set_index("object_id")
    topic_config_i = topic_config.set_index("features")
    for topic in topic_config["topic"].unique():
        t_topic = topic_config_i[topic_config_i["topic"] == topic]
        joined_df = features_indexed_df.join(t_topic, on="features")
        g = joined_df.groupby(level=0)
        df = g.agg({"features_completeness": "sum", "topic": "first"})
        df = df[df["features_completeness"] >= 1]
        dfs.append(df.drop(columns="features_completeness"))

    result_df = pd.concat(dfs)
    return result_df.reset_index()


def get_topic_config(config_url=None) -> pd.DataFrame:
    """Get topic config dataframe."""
    df = _get_raw_topic_config(config_url)
    df["topic"] = df["Topic"].str.lower()
    df["features"] = df["Features"].str.split(",")
    df["features_count"] = df["features"].str.len()
    df_ex = df.explode("features", ignore_index=True)
    df_ex["features_completeness"] = 1 / df_ex["features_count"]
    return df_ex[["features", "topic", "features_completeness"]]


def _get_raw_topic_config(config_url=None) -> pd.DataFrame:
    """Get the raw topic_config."""
    if not config_url:
        config_url = f"{artifacts.urls.get_static_config()}{DEFAULT_RAW_TOPIC_CONFIG}"

    with tentaclio.open(config_url, "r") as fb:
        df = pd.read_csv(fb)
    return df


    ValueError("Currently, only none config_urls are supported for raw_topic_config.")
