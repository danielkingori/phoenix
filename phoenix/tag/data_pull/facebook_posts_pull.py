"""Data pulling for facebook posts."""
import hashlib

import pandas as pd
import tentaclio

from phoenix.tag.data_pull import constants, utils


def from_csv(url_to_folder: str) -> pd.DataFrame:
    """Get all the csvs and return a normalised facebook posts."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        with tentaclio.open(entry) as file_io:
            df = pd.read_csv(file_io, index_col=None, header=0)
            li.append(df)

    posts_df = pd.concat(li, axis=0, ignore_index=True)
    df = normalise(posts_df)
    # Get the most recent post
    # There are some cases where there is a message and link that is the same but
    # some other data has changed.
    # In this case we get the latest pulled value
    df = df.groupby("phoenix_post_id").last()
    df = df.reset_index()
    return df


def normalise(raw_df: pd.DataFrame):
    """normalise_fb_posts raw dataframe."""
    df = raw_df.rename(utils.snake_names, axis="columns")
    df = df[~df["message"].isna()]
    df = utils.to_type("message", str, df)
    df = utils.to_type("page_description", str, df)
    df = process_edt_datetime("page_created", df)
    df = process_edt_datetime("post_created", df)
    # This will be hashed so that links are in the hash
    df["message_link"] = df["message"] + "-" + df["link"].fillna("")
    df["message_hash"] = df["message_link"].apply(hash_message)
    df["post_created_date"] = pd.to_datetime(df["post_created_date"], format="%Y-%m-%d").dt.date
    df["total_interactions"] = (
        df["total_interactions"].replace(to_replace=r",", value="", regex=True).astype(int)
    )
    # There is no post id from the csv export
    # So we are making one from the account that posted it and a hash of the message
    df["phoenix_post_id"] = df["facebook_id"].astype(str) + "-" + df["message_hash"].astype(str)
    return df


def hash_message(message: str):
    """Get the has of a message."""
    return hashlib.md5(bytes(message, "utf-8")).hexdigest()[:16]


def process_edt_datetime(column_name: str, df: pd.DataFrame):
    """Name column string."""
    df[column_name + "_back"] = df[column_name]
    df[column_name + "_utc"] = df[column_name].str[-3:]
    df[column_name] = df[column_name].str[:-4]
    utc_format = "%Y-%m-%d %H:%M:%S"
    df[column_name] = pd.to_datetime(df[column_name], format=utc_format)
    # US/Eastern is hard coded
    df[column_name] = df[column_name].dt.tz_localize("US/Eastern").dt.tz_convert("UTC")
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
    # Twitter has a language column
    if "lang" in given_df.columns:
        df["language_from_api"] = given_df["lang"]

    df = df.rename(columns={"phoenix_post_id": "object_id", "message": "text"})
    df = df.set_index(df["object_id"], verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_FACEBOOK_POST
    return df
