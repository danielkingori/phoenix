"""Data pulling for facebook posts."""
from typing import Optional

import json
import logging

import numpy as np
import pandas as pd
import tentaclio

from phoenix.common import constants as common_constants
from phoenix.common import pd_utils
from phoenix.tag.data_pull import constants, utils


def from_json(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Get all the JSON files and return a normalised facebook posts."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        if not utils.is_valid_file_name(entry):
            logging.info(f"Skipping file with invalid filename: {entry}")
            continue
        file_timestamp = utils.get_file_name_timestamp(entry)
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
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df = df.sort_values("file_timestamp")
    df = df.groupby("phoenix_post_id").last()
    df = df.reset_index()
    if year_filter:
        df = df[df["year_filter"] == year_filter]
    if month_filter:
        df = df[df["month_filter"] == month_filter]
    return df


def normalise(raw_df: pd.DataFrame, df_flattened: pd.DataFrame) -> pd.DataFrame:
    """Normalise the raw dataframe.

    The raw data is in JSON format and has a number of nested properties. As such
    we process two versions of raw data.

    1. raw_df: has the nested data, this uses `pd.read_json`
    2. df_flattened: this has all of the nested data flattened using `pd.json_normalize`

    The reason for this is ease of creating the final data structure:
    - `raw_df` (`pd.read_json`) has better dtypes for the non nested properties
    - `df_flattened` (`pd.json_normalize`) has the formatted columns for the nested data

    Args:
        raw_df: return of `pd.read_json` of the source file
        df_flattened: return of `pd.json_normalize` of the source file
    """
    df = raw_df.rename(pd_utils.camel_to_snake, axis="columns")
    df = df.rename(columns={"language_code": "language_from_api", "message": "text"})
    df_flattened = df_flattened.rename(pd_utils.camel_to_snake, axis="columns")
    df_flattened.columns = df_flattened.columns.str.replace(".", "_")
    df = merge_flattened(df, df_flattened)
    df = df[~df["text"].isna()]
    df = utils.to_type("text", str, df)
    df = utils.to_type("type", str, df)
    df = map_score(common_constants.FACEBOOK_POST_SORT_BY, df)
    df["post_created"] = df["date"].dt.tz_localize("UTC")
    df["timestamp_filter"] = df["post_created"]
    df["date_filter"] = df["post_created"].dt.date
    df["year_filter"] = df["post_created"].dt.year
    df["month_filter"] = df["post_created"].dt.month
    df["day_filter"] = df["post_created"].dt.day
    df["updated"] = pd.to_datetime(df["updated"]).dt.tz_localize("UTC")
    # This will be hashed so that links are in the hash
    df["text_link"] = df["text"] + "-" + df["link"].fillna("")
    df["text_hash"] = df["text_link"].apply(utils.hash_message)
    df["scrape_url"] = df["post_url"].str.replace(
        "https://www.facebook", "https://mbasic.facebook"
    )
    # URL post id
    df["url_post_id"] = df["post_url"].fillna("").str.extract(r"(\d+$)", flags=0, expand=False)
    # Still using the phoenix_post_id as this seems a good way of identifying posts
    # So we are making one from the account that posted it and a hash of the message
    df["phoenix_post_id"] = (
        df["account_platform_id"].astype(str) + "-" + df["text_hash"].astype(str)
    ).astype(str)
    return df.drop(
        columns=[
            "account",
            "statistics",
            "media",
            "expanded_links",
            "branded_content_sponsor",
            "live_video_status",
            "legacy_id",
        ],
        # Using ignore as missing data is not imporant
        errors="ignore",
    )


def map_score(sort_by_api: str, df: pd.DataFrame) -> pd.DataFrame:
    """Map score based on the sort by parameter of the request."""
    all_scores_mapping = {
        "total_interactions": "total_interactions",
        "overperforming": "overperforming_score",
        "interaction_rate": "interaction_rate",
        "underperforming": "underperforming_score",
    }
    for sort_by_match, col in all_scores_mapping.items():
        if sort_by_match == sort_by_api:
            df.rename(columns={"score": col}, inplace=True)
        else:
            df[col] = np.nan
        df[col] = df[col].astype(float)

    return df


def merge_flattened(df: pd.DataFrame, df_flattened: pd.DataFrame) -> pd.DataFrame:
    """Merged flattened dataframe with the non flattened.

    1. df: has the nested data, this uses `pd.read_json`
    2. df_flattened: this has all of the nested data flattened using `pd.json_normalize`

    The reason for this is ease of creating the final data structure:
    - `df` (`pd.read_json`) has better dtypes for the non nested properties
    - `df_flattened` (`pd.json_normalize`) has the formatted columns for the nested data

    Args:
        df: return of `pd.read_json` of the source file
        df_flattened: return of `pd.json_normalize` of the source file
    """
    to_add = [
        "account_name",
        "account_handle",
        "account_platform_id",
        "account_page_category",
        "account_page_admin_top_country",
        "account_page_description",
        "account_url",
        "account_page_created_date",
        "statistics_actual_like_count",
        "statistics_actual_comment_count",
        "statistics_actual_share_count",
        "statistics_actual_love_count",
        "statistics_actual_wow_count",
        "statistics_actual_haha_count",
        "statistics_actual_sad_count",
        "statistics_actual_angry_count",
        "statistics_actual_care_count",
    ]
    df[to_add] = df_flattened[to_add]
    # Some posts don't have an account this should be looked in to further
    # https://gitlab.com/howtobuildup/phoenix/-/issues/47
    df["account_platform_id"] = df["account_platform_id"].fillna(0).astype(int)
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
        created_at: datetime
        object_url: String, dtype: string
        object_user_url: String, dtype: string

    """
    df = given_df.copy()
    df = df[
        ["phoenix_post_id", "text", "language_from_api", "post_created", "post_url", "account_url"]
    ]

    df = df.rename(
        columns={
            "phoenix_post_id": "object_id",
            "post_created": "created_at",
            "post_url": "object_url",
            "account_url": "object_user_url",
        }
    )
    df = df.set_index(df["object_id"], verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_FACEBOOK_POST
    return df
