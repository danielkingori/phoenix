"""Data pulling for twitter."""
import pandas as pd
import tentaclio

from phoenix.tag.data_pull import constants, utils


def twitter_json(url_to_folder: str) -> pd.DataFrame:
    """Get all the csvs and return a dataframe with tweet data."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        with tentaclio.open(entry) as file_io:
            df = pd.read_json(file_io)
            li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    # Get the most recent tweet
    df = df.groupby("id_str").last()
    df = df.reset_index()
    return normalise_json(df)


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
