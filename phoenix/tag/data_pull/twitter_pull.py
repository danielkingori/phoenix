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

        # Guard against empty files
        if df.shape[0] == 0:
            continue

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
    df = add_medium_type_and_determinants(df)
    df = df.rename(columns={"full_text": "text", "lang": "language_from_api"})
    df = user_normalise(df)
    df = retweeted_status_normalise(df)
    # Dropping nested data for the moment
    df = df.drop(
        columns=[
            "entities",
            "user",
            "extended_entities",
            "place",
            "retweeted_status",
            "quoted_status",
        ],
        errors="ignore",
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


def retweeted_status_normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise retweet data and join back to input df."""
    if "retweeted_status" not in df.columns:
        return df
    retweet_df = df[["retweeted_status"]].copy()
    retweet_df["retweeted_status"] = retweet_df["retweeted_status"].map(
        lambda x: {} if pd.isnull(x) else x
    )
    retweet_df = pd.json_normalize(retweet_df["retweeted_status"])
    retweet_df["retweeted_user_screen_name"] = retweet_df["user.screen_name"]
    return df.join(retweet_df[["retweeted_user_screen_name"]])


def get_medium_type_and_determinants(row: pd.Series) -> str:
    """Map the medium type and it's determinants."""
    type_of_first_media = get_type_of_first_media(row)
    url_of_first_entity = get_url_of_first_entity(row)
    medium_type = get_medium_type(type_of_first_media, url_of_first_entity)

    return pd.Series([medium_type, type_of_first_media, url_of_first_entity])


def get_medium_type(type_of_first_media: Optional[str], url_of_first_entity: Optional[str]) -> str:
    """Get the medium_type."""
    if type_of_first_media == "video":
        return constants.MEDIUM_TYPE_VIDEO

    if type_of_first_media in ["photo", "animated_gif"]:
        return constants.MEDIUM_TYPE_PHOTO

    if url_of_first_entity:
        return constants.MEDIUM_TYPE_LINK

    return constants.MEDIUM_TYPE_TEXT


def get_type_of_first_media(row: pd.Series) -> Optional[str]:
    """Get the media of the first media."""
    includes = {}
    if "includes" in row:
        includes = row["includes"]

    media = includes.get("media")
    type_of_first_media = None
    if media and len(media) > 0:
        type_of_first_media = media[0].get("type")

    return type_of_first_media


def get_url_of_first_entity(row: pd.Series) -> Optional[str]:
    """Get the urls of the first entity."""
    entities = {}
    if "entities" in row:
        entities = row["entities"]
    urls = entities.get("urls")
    url_of_first_url = None
    if urls and len(urls) > 0:
        url_of_first_url = urls[0].get("url")

    return url_of_first_url


def add_medium_type_and_determinants(df: pd.DataFrame) -> pd.DataFrame:
    """Add the medium_type and it's determinants to the dataframe."""
    medium_type_series = df.apply(get_medium_type_and_determinants, axis=1)
    df[["medium_type", "platform_media_type", "url_within_text"]] = medium_type_series
    return df


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
        created_at: datetime
        object_url: String, dtype: string
        object_user_url: String, dtype: string
        object_user_name: String, dtype: string

    """
    df = given_df.copy()
    df = df[["id_str", "id", "text", "language_from_api", "created_at", "user_screen_name"]]
    df["object_user_url"] = constants.TWITTER_URL + df["user_screen_name"].astype(str)
    df["object_url"] = constants.TWITTER_URL + "i/web/status/" + df["id"].astype(str)
    df = df.drop(["id"], axis=1)

    if "retweeted" in given_df.columns:
        df["retweeted"] = given_df["retweeted"]

    df = df.rename(columns={"id_str": "object_id", "user_screen_name": "object_user_name"})
    df = df.set_index("object_id", drop=False, verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_TWEET
    return df
