"""Single feature match for topic analysis."""
import logging

import arabic_reshaper
import matplotlib.pyplot as plt
import pandas as pd
from bidi.algorithm import get_display
from IPython.display import display


FILL_TOPIC = "irrelevant"

logger = logging.getLogger(__name__)


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


def get_topics_analysis_df(topics_df: pd.DataFrame) -> pd.DataFrame:
    """Process the output of get_topics function for analysis purposes."""
    analysis_df = (
        topics_df.explode("matched_features")
        .groupby(["topic", "matched_features"])
        .size()
        .reset_index(name="matched_object_count")
    )
    analysis_df = analysis_df.sort_values(
        ["topic", "matched_object_count"], ascending=(True, False)
    ).reset_index(drop=True)
    analysis_df["matched_features"] = analysis_df["matched_features"].apply(
        lambda x: get_display(arabic_reshaper.reshape(x))
    )
    return analysis_df


def analyse(topics_df: pd.DataFrame) -> pd.DataFrame:
    """Analyse the outputs of the get_topics function."""
    analysis_df = get_topics_analysis_df(topics_df)
    logger.info("Top 3 features per topic:\n")
    display(analysis_df.groupby("topic").head(3))
    logger.info("Line bars of distributions of matched features:\n")
    grouped = analysis_df.groupby("topic")
    for name, group in grouped:
        group.plot.bar(x="matched_features", y="matched_object_count")
        plt.title(str(name), fontsize=15)
    display(plt.show())
    return analysis_df
