"""Single feature match for topic analysis."""
import numpy as np
import pandas as pd


FILL_TOPIC = "irrelevant"


def get_topics(topic_config, features_df, fill_topic: str = FILL_TOPIC) -> pd.DataFrame:
    """Get the topics.

    Arguments:
        topic_config: see phoenix/tag/topic/single_feature_match_topic_config.py
        features_df: data frame with schema docs/schemas/features.md
        fill_topic: the topic that will be given to all objects that don't have a topic.
            Default is "irrelevant".

    Return:
        pd.DataFrame with schema: docs/schemas/topics.md
    """
    features_indexed_df = features_df.set_index("object_id")
    topic_config_i = topic_config.set_index("features")
    topics_df = features_indexed_df.join(topic_config_i, on="features")
    no_topic = topics_df[topics_df["topic"].isnull()]
    topics_df = topics_df[~topics_df["topic"].isnull()]
    topics_df = (
        topics_df.groupby(["object_id", "topic", "object_type"])
        .agg({"features": list})
        .rename(columns={"features": "matched_features"})
    )
    topics_df["has_topic"] = True
    # Adding the FILL_TOPIC to all the objects that don't have a topic
    no_topic = no_topic[~no_topic.index.isin(topics_df.index.get_level_values(0))]
    no_topic = no_topic.reset_index()
    no_topic = no_topic[["object_id", "object_type"]].drop_duplicates()
    no_topic["topic"] = fill_topic
    no_topic["matched_features"] = None
    no_topic["has_topic"] = False
    topics_df = topics_df.reset_index()

    return pd.concat([topics_df, no_topic], ignore_index=True)


def get_matched_features_count_for_topic(topics_df, topic):
    """Get the matched_features count for a topic."""
    topics_filtered_df = topics_df[topics_df["topic"] == topic]
    uniques, counts = np.unique(
        sum(list(topics_filtered_df["matched_features"]), []), return_counts=True
    )
    matched_features = pd.DataFrame({"matched_feature": uniques, "count": counts})
    matched_features = matched_features.reset_index()
    return matched_features.sort_values(by=["count"], ascending=False)
