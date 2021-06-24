"""Functionallity for adding to the single feature match topic config."""
import pandas as pd
import tentaclio

from phoenix.common import artifacts
from phoenix.tag.topic import constants


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
        config_url = f"{artifacts.urls.get_static_config()}{constants.DEFAULT_RAW_TOPIC_CONFIG}"

    with tentaclio.open(config_url, "r") as fb:
        df = pd.read_csv(fb)
    return df
