"""Data pulling for facebook posts."""
import datetime
import json
import os

import pandas as pd
import tentaclio

from phoenix.tag.data_pull import constants, utils


def from_json(url_to_folder: str) -> pd.DataFrame:
    """Get all the JSON files and return a normalised facebook posts."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        file_timestamp = get_file_name_timestamp(entry)
        # Getting both the normalised and the read
        # This is because the read does a better job of typing
        # We are going to join them together in the normalisation
        with tentaclio.open(entry) as file_io:
            js_obj = json.load(file_io)
            df_flattened = pd.json_normalize(js_obj)

        with tentaclio.open(entry) as file_io:
            df_read = pd.read_json(file_io)

        df = normalise(df_read, df_flattened)
        df["file_timestamp"] = file_timestamp
        df["file_timestamp"] = df["file_timestamp"].dt.tz_localize("UTC")
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df = df.sort_values("file_timestamp")
    df = df.groupby("phoenix_post_id").last()
    df = df.reset_index()
    return df


def get_file_name_timestamp(url: str) -> datetime.datetime:
    """Get the timestamp in the file name."""
    suffix = "posts-"
    file_name, _ = os.path.splitext(os.path.basename(url))
    if file_name.startswith(suffix):
        timestamp_str = file_name[len(suffix) :]
        # The files in google drive have : replaced with _
        timestamp_str = timestamp_str.replace("_", ":")
        return datetime.datetime.fromisoformat(timestamp_str)

    return datetime.datetime.now()


def normalise(raw_df: pd.DataFrame, df_flattened: pd.DataFrame) -> pd.DataFrame:
    """Normalise the raw dataframe."""
    df = raw_df.rename(utils.camel_to_snake, axis="columns")
    df_flattened = df_flattened.rename(utils.camel_to_snake, axis="columns")
    df_flattened.columns = df_flattened.columns.str.replace(".", "_")
    df = df[~df["message"].isna()]
    df = utils.to_type("message", str, df)
    df = utils.to_type("type", str, df)
    df["post_created"] = df["date"].dt.tz_localize("UTC")
    df["updated"] = pd.to_datetime(df["updated"]).dt.tz_localize("UTC")
    # This will be hashed so that links are in the hash
    df["message_link"] = df["message"] + "-" + df["link"].fillna("")
    df["message_hash"] = df["message_link"].apply(utils.hash_message)
    df["scrape_url"] = df["post_url"].str.replace(
        "https://www.facebook", "https://mbasic.facebook"
    )
    # URL post id
    df["url_post_id"] = df["post_url"].fillna("").str.extract(r"(\d+$)", flags=0, expand=False)
    # Still using the phoenix_post_id as this seems a good way of identifying posts
    # So we are making one from the account that posted it and a hash of the message
    df["phoenix_post_id"] = (
        df_flattened["account_platform_id"].astype(str) + "-" + df["message_hash"].astype(str)
    ).astype(str)
    df = merge_flattened(df, df_flattened)
    return df.drop(
        columns=[
            "account",
            "statistics",
            "media",
            "expanded_links",
            "branded_content_sponsor",
            "live_video_status",
            "legacy_id",
        ]
    )


def merge_flattened(df: pd.DataFrame, df_flattened: pd.DataFrame) -> pd.DataFrame:
    """Merged to flattened dataframe with the non flattened.

    Doing this because the flattened dataframe has incorrect types.
    """
    to_add = [
        "account_platform_id",
        "account_page_category",
        "account_page_admin_top_country",
        "account_page_description",
        "account_url",
        "account_page_created_date",
        "statistics_actual_like_count",
        "statistics_actual_comment_count",
        "statistics_actual_share_count",
        "statistics_actual_wow_count",
        "statistics_actual_haha_count",
        "statistics_actual_sad_count",
        "statistics_actual_angry_count",
        "statistics_actual_care_count",
    ]
    df[to_add] = df_flattened[to_add]
    df["account_platform_id"] = df["account_platform_id"].astype(int)
    df["account_page_created_date"] = pd.to_datetime(
        df["account_page_created_date"]
    ).dt.tz_localize("UTC")
    to_str = [
        "account_page_category",
        "account_page_admin_top_country",
        "account_page_description",
    ]

    for col in to_str:
        df[col] = df[col].astype(str)
    return df


def for_tagging(given_df: pd.DataFrame):
    """Get facebook posts for tagging.

    Return:
    dataframe  : pandas.DataFrame
    Index:
        object_id: String, dtype: string
    Columns:
        object_id: String, dtype: string
        text: String, dtype: string
        object_type: "facebook_post", dtype: String

    """
    df = given_df.copy()
    df = df[["phoenix_post_id", "message"]]
    column_with_lang_from_api = "langauge_code"
    if column_with_lang_from_api in given_df.columns:
        df["language_from_api"] = given_df[column_with_lang_from_api]

    df = df.rename(columns={"phoenix_post_id": "object_id", "message": "text"})
    df = df.set_index(df["object_id"], verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_FACEBOOK_POST
    return df
