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
