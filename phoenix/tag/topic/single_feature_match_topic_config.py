"""Functionality for processing single feature match topic config."""
import pandas as pd
import tentaclio

from phoenix.common import artifacts
from phoenix.tag.topic import constants


def get_topic_config(config_url=None) -> pd.DataFrame:
    """Get topic config dataframe.

    Arguments:
        config_url: to use a non default config.

    Returns:
        topics_config: see docs/schemas/topic_config.md
    """
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
        config_url = _default_config_url()

    with tentaclio.open(config_url, "r") as fb:
        df = pd.read_csv(fb)
    return df


def merge_new_topic_config(topic_config, new_topic_config) -> pd.DataFrame:
    """Merge a new topic config with the old one.

    The new will replace add all new topic to feature mappings.

    Arguments:
        topic_config: dataframe in the schema docs/schemas/topic_config.md
        new_topic_config: dataframe in the schema docs/schemas/topic_config.md

    Returns:
       dataframe with schema docs/schemas/topic_config.md
    """
    n_df = pd.concat([topic_config, new_topic_config])
    n_df = n_df.drop_duplicates()
    n_df = n_df[~n_df["topic"].isnull()]
    n_df = n_df.reset_index(drop=True)
    return n_df


def committable_topic_config(topic_config: pd.DataFrame) -> pd.DataFrame:
    """Create a committable a topic config.

    This will create the topic config in the format
    features, topic
    "f1", "t1,t2"

    As this is easier to work with when labelling by hand.
    Arguments:
        topic_config: dataframe in the schema docs/schemas/topic_config.md

    Returns:
        Persisted Topic Config:
            dataframe in the schema docs/schemas/persist_topic_config_csv
    """
    df = topic_config.groupby("features", as_index=False).agg(
        {"topic": lambda x: ", ".join(sorted(x.dropna()))}
    )
    return df


def persist_topic_config_csv(df: pd.DataFrame, config_url=None):
    """Persist the topic config as a csv.

    Arguments:
        df: dataframe in the schema docs/schemas/persist_topic_config_csv

    Returns:
        None
    """
    if not config_url:
        config_url = _default_config_url()
    with tentaclio.open(config_url, "w") as fb:
        df.to_csv(fb)


def _default_config_url() -> str:
    """Default config url."""
    return f"{artifacts.urls.get_static_config()}{constants.DEFAULT_RAW_TOPIC_CONFIG}"


def create_new_committable_topic_config(
    topic_config: pd.DataFrame, url_to_folder: str
) -> pd.DataFrame:
    """Create the new committable topic config by merging the csvs in the folder to it.

    Arguments:
        topic_config: dataframe in the schema docs/schemas/topic_config.md
        url_to_folder: URL of the folder of new persisted topic configs to merge.

    Returns:
        Persisted Topic config:
            dataframe in the schema docs/schemas/persist_topic_config_csv
    """
    current_topic_config = topic_config.copy()
    for entry in tentaclio.listdir(url_to_folder):
        new_topic_config = get_topic_config(entry)
        current_topic_config = merge_new_topic_config(current_topic_config, new_topic_config)

    return committable_topic_config(current_topic_config)
