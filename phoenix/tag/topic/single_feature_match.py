"""Single feature match for topic analysis."""
import pandas as pd


def get_topics(topic_config, features_df) -> pd.DataFrame:
    """Get the topics.

    Return:
    pd.DataFrame:
        object_id: object_id
        object_type: object_type
        topic: string
        matched_features: array<string>
    """
    features_indexed_df = features_df.set_index("object_id")
    topic_config_i = topic_config.set_index("features")
    topics_df = features_indexed_df.join(topic_config_i, on="features")
    topics_df = topics_df[~topics_df["topic"].isnull()]
    topics_df = (
        topics_df.groupby(["object_id", "topic", "object_type"])
        .agg({"features": list})
        .rename(columns={"features": "matched_features"})
    )
    return topics_df.reset_index()
