"""Topic tagging."""
import pandas as pd


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
    pass
