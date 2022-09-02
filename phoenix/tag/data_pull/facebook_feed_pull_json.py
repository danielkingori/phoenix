"""Data pulling for facebook posts."""
from typing import Optional

import logging

import pandas as pd
import tentaclio

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
        with tentaclio.open(entry) as file_io:
            df_read = pd.read_json(file_io, dtype={"id": str})

        # Gaurd against empty files
        if df_read.shape[0] == 0:
            continue

        df = normalise(df_read)
        df["file_timestamp"] = file_timestamp
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df = df.sort_values(by=["updated_time", "file_timestamp"])
    df = df.groupby("phoenix_post_id").last()
    df = df.reset_index()
    if year_filter:
        df = df[df["year_filter"] == year_filter]
    if month_filter:
        df = df[df["month_filter"] == month_filter]
    return df


def normalise(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the raw dataframe."""
    df = raw_df.rename(pd_utils.camel_to_snake, axis="columns")
    df = df.rename(columns={"message": "text"})
    df = df[~df["text"].isna()]
    df = utils.to_type("text", str, df)
    df["post_created"] = df["created_time"]
    df["timestamp_filter"] = df["post_created"]
    df["date_filter"] = df["post_created"].dt.date
    df["year_filter"] = df["post_created"].dt.year
    df["month_filter"] = df["post_created"].dt.month
    df["day_filter"] = df["post_created"].dt.day
    df["updated"] = df["updated_time"]
    df["post_url"] = "https://www.facebook.com/" + df["id"]
    df_from = pd.DataFrame(df["from"].values.tolist())
    df["account_id"] = df_from["id"]
    df["account_name"] = df_from["name"]
    df["account_url"] = "https://www.facebook.com/" + df["account_id"]
    # Still using the phoenix_post_id as this seems a good way of identifying posts
    # So we are making one from the account that posted it and a hash of the message
    df["phoenix_post_id"] = df["id"].astype(str)
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
        object_user_name: String, dtypu: string

    """
    df = given_df.copy()
    df = df[
        [
            "phoenix_post_id",
            "text",
            "post_created",
            "post_url",
            "account_url",
            "account_id",
            "account_name",
        ]
    ]

    df = df.rename(
        columns={
            "phoenix_post_id": "object_id",
            "post_created": "created_at",
            "post_url": "object_url",
            "account_url": "object_user_url",
            "account_name": "object_user_name",
        }
    )
    df = df.set_index("object_id", drop=False, verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_FACEBOOK_POST
    return df
