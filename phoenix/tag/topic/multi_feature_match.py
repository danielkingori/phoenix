"""Multi feature match for topic analysis.

This is depreciated and does not work as expected.

Please see tests/unit/tag/topic/test_multi_feature_match.py for
more information.
"""
from typing import Any, Dict

import pandas as pd
import tentaclio

from phoenix.common import artifacts


DEFAULT_RAW_TOPIC_CONFIG = "multi_feature_match_topic_config.csv"


def get_topics(topic_config, features_df, with_features_and_text=False) -> pd.DataFrame:
    """Get the topics.

    Return:
    pd.DataFrame:
        Index: object_id
        topic: string
    """
    dfs = []
    confidence_dfs = []
    features_indexed_df = features_df.set_index("object_id")
    topic_config_i = topic_config.set_index("features")
    for topic in topic_config["topic"].unique():
        t_topic = topic_config_i[topic_config_i["topic"] == topic]
        joined_df = features_indexed_df.join(t_topic, on="features")
        g = joined_df.groupby(level=0)
        aggregation_conf: Dict[str, Any] = {"features_completeness": "sum", "topic": "first"}
        if with_features_and_text:
            aggregation_conf = {**aggregation_conf, "features": list, "text": "first"}

        df = g.agg(aggregation_conf)
        df["topic"] = df["topic"].fillna(topic)
        confidence_dfs.append(df.rename(columns={"features_completeness": "confidence"}))
        df = df[df["features_completeness"] >= 1]
        dfs.append(df.drop(columns="features_completeness"))

    result_df = pd.concat(dfs)
    confidence_df = pd.concat(confidence_dfs)
    return result_df.reset_index(), confidence_df.reset_index()


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
        config_url = f"{artifacts.urls.get_static_data()}{DEFAULT_RAW_TOPIC_CONFIG}"

    with tentaclio.open(config_url, "r") as fb:
        df = pd.read_csv(fb)
    return df


def get_object_topics(topics_df) -> pd.DataFrame:
    """Get topics grouped by object."""
    return topics_df.groupby("object_id").agg(list).reset_index()
