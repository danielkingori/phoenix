"""Data pulling for twitter."""
from typing import Optional

import logging

import pandas as pd
import tentaclio

from phoenix.tag.data_pull import constants, utils


def twitter_json(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Get all the csvs and return a dataframe with tweet data."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        file_timestamp = utils.get_file_name_timestamp(entry)
        with tentaclio.open(entry) as file_io:
            df = pd.read_json(file_io)
            li.append(df)

        df["file_timestamp"] = file_timestamp

    df = pd.concat(li, axis=0, ignore_index=True)
    df = df.sort_values("file_timestamp")
    df = df.groupby("id_str").last()
    df = df.reset_index()
    df = normalise_json(df)
    if year_filter:
        df = df[df["year_filter"] == year_filter]
    if month_filter:
        df = df[df["month_filter"] == month_filter]
    return df


def normalise_json(raw_df: pd.DataFrame):
    """normalise_tweets raw dataframe."""
    df = utils.to_type("full_text", str, raw_df)
    df = utils.to_type("id_str", str, raw_df)
    df["timestamp_filter"] = df["created_at"]
    df["date_filter"] = df["created_at"].dt.date
    df["year_filter"] = df["created_at"].dt.year
    df["month_filter"] = df["created_at"].dt.month
    df["day_filter"] = df["created_at"].dt.day
    df = df.rename(columns={"full_text": "text", "lang": "language_from_api"})
    df = user_normalise(df)
    # Dropping nested data for the moment
    df = df.drop(
        columns=[
            "entities",
            "user",
            "extended_entities",
            "place",
            "retweeted_status",
            "quoted_status",
        ]
    )
    return df


def user_normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the user."""
    user_columns = [
        "id",
        "id_str",
        "name",
        "screen_name",
        "location",
        "description",
        "url",
        "protected",
        "created_at",
        "geo_enabled",
        "verified",
        "lang",
    ]

    user_df = pd.json_normalize(df["user"])
    user_df = user_df[user_columns]
    user_df["created_at"] = pd.to_datetime(df["created_at"])
    user_df = user_df.add_prefix("user_")
    return df.join(user_df)


def for_tagging(given_df: pd.DataFrame):
    """Get tweets for tagging.

    Return:
    dataframe  : pandas.DataFrame
    Index:
        object_id: String, dtype: string
    Columns:
        object_id: String, dtype: string
        text: String, dtype: string
        object_type: "tweet", dtype: String

    """
    df = given_df.copy()
    df = df[["id_str", "text", "language_from_api"]]

    if "retweeted" in given_df.columns:
        df["retweeted"] = given_df["retweeted"]

    df = df.rename(columns={"id_str": "object_id"})
    df = df.set_index(df["object_id"], verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_TWEET
    return df
