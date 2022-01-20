"""Single feature match for topic analysis."""
from typing import Optional

import logging

import arabic_reshaper
import matplotlib.pyplot as plt
import pandas as pd
from bidi.algorithm import get_display
from IPython.display import display


FILL_TOPIC = "irrelevant"

logger = logging.getLogger(__name__)


def get_topics(
    topic_config: pd.DataFrame,
    features_df: pd.DataFrame,
    unprocessed_features_df: Optional[pd.DataFrame] = None,
    fill_topic: str = FILL_TOPIC,
) -> pd.DataFrame:
    """Get the topics for objects based on the topic config.

    If unprocessed_features_df is added AND the column `use_processed_features` (dtype: bool) is in
    the topic_config df, it will use that flag to match the topic based on the topic_config's
    processed_features with the `features_df` and the topic_config's `unprocessed_features` with
    the `unprocessed_features_df`.

    Arguments:
        topic_config: see phoenix/tag/topic/single_feature_match_topic_config.py
        features_df: data frame with schema docs/schemas/features.md
        fill_topic: the topic that will be given to all objects that don't have a topic.
            Default is "irrelevant".
        unprocessed_features_df (Optional[pd.DataFrame]): dataframe with schema SFLM Necessary
            Features docs/schemas/features.md

    Return:
        pd.DataFrame with schema: docs/schemas/topics.md
    """
    if unprocessed_features_df is not None and "use_processed_features" in topic_config.columns:
        unprocessed_features_topics_df = join_features_on_topic_config(
            unprocessed_features_df,
            topic_config[~topic_config["use_processed_features"]],
            "unprocessed_features",
        )
        processed_features_topics_df = join_features_on_topic_config(
            features_df, topic_config[topic_config["use_processed_features"]]
        )
        topics_df = pd.concat([unprocessed_features_topics_df, processed_features_topics_df])
    else:
        topics_df = join_features_on_topic_config(features_df, topic_config)
    topics_df = process_joined_topics(fill_topic, topics_df)
    return topics_df


def join_features_on_topic_config(
    features_df: pd.DataFrame,
    topic_config: pd.DataFrame,
    topic_config_join_column: str = "features",
):
    """Join features on a column in the topic_config."""
    topic_config_copy = topic_config.copy()
    if topic_config_join_column != "features":
        topic_config_copy = topic_config_copy.drop(columns=["features"])
        topic_config_copy = topic_config_copy.rename(
            {topic_config_join_column: "features"}, axis=1
        )
    features_indexed_df = features_df.set_index("object_id")
    topic_config_i = topic_config_copy.set_index("features")

    topics_df = features_indexed_df.join(topic_config_i, on="features")
    return topics_df[["features", "object_type", "topic"]]


def process_joined_topics(fill_topic: str, topics_df: pd.DataFrame):
    """Process joined topics by aggregating matched features, and fill nulls."""
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
