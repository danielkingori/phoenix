"""Data pulling for facebook comments."""
import itertools
import json
import logging

import pandas as pd
import tentaclio

from phoenix.tag.data_pull import constants, utils


def from_json(url_to_folder: str) -> pd.DataFrame:
    """Get all the jsons and return a normalised facebook comments."""
    comment_li = []
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        if not utils.is_valid_file_name(entry):
            logging.info(f"Skipping file with invalid filename: {entry}")
            continue
        file_timestamp = utils.get_file_name_timestamp(entry)
        with tentaclio.open(entry) as file_io:
            pages = json.load(file_io)
            comments_df = get_comments_df(pages)

        comments_df["file_timestamp"] = file_timestamp
        comment_li.append(comments_df)

    df = pd.concat(comment_li, axis=0, ignore_index=True)
    df = df.sort_values("file_timestamp")
    df = df.groupby("id").last()
    df = df.reset_index()
    return normalise_comments_dataframe(df)


def get_comments_df(pages):
    """Get the comments dataframe from pages."""
    comments = list(itertools.chain.from_iterable([get_comments(page) for page in pages]))
    return pd.DataFrame(comments)


def get_comments(page_json):
    """Get comments from page."""
    # In an early version of the comment parser the output artifact
    # had json within a json. This was a bug that has now been fixed.
    # Here we are still supporting the processing of the legacy format.
    if isinstance(page_json, str):
        page = json.loads(page_json)
    else:
        page = page_json
    return [normalise_comment(comment, page) for comment in page["comments"]]


def legacy_normalise_comment(comment, page):
    """Legacy Normalise comment."""
    return {
        "id": comment["fb_id"],
        "post_id": comment["top_level_post_id"],
        "file_id": comment["file_id"],
        "parent_id": comment["parent"],
        "post_created": comment["date_utc"],
        "text": comment["text"],
        "reactions": 0 if comment["reactions"] == "" else comment["reactions"],
        "top_sentiment_reactions": comment["sentiment"],
        "user_display_name": comment["display_name"],
        "user_name": comment["username"],
        "position": comment["position"],
    }


def normalise_comment(comment, page):
    """Normalise comment."""
    if "fb_id" in comment:
        return legacy_normalise_comment(comment, page)
    return {
        "id": comment["id"],
        "post_id": comment["post_id"],
        "file_id": comment["file_id"],
        "parent_id": comment["parent"],
        "post_created": comment["date_utc"],
        "text": comment["text"],
        "reactions": 0 if comment["reactions"] == "" else comment["reactions"],
        "top_sentiment_reactions": comment["top_sentiment_reactions"],
        "user_display_name": comment["user_display_name"],
        "user_name": comment["user_name"],
        "position": comment["position"],
    }


def normalise_comments_dataframe(df):
    """Normalise the comments data frame."""
    df["id"] = df["id"].astype(int)
    df["post_id"] = df["post_id"].astype(int)
    df["file_id"] = df["file_id"].astype(str)
    df["parent_id"] = df["parent_id"].astype(int)
    df["post_created"] = pd.to_datetime(df["post_created"]).dt.tz_localize("UTC")
    df["timestamp_filter"] = df["post_created"]
    df["date_filter"] = df["post_created"].dt.date
    df["year_filter"] = df["post_created"].dt.year
    df["month_filter"] = df["post_created"].dt.month
    df["day_filter"] = df["post_created"].dt.day
    df["text"] = df["text"].astype(str)
    df["reactions"] = df["reactions"].astype(int)
    df["user_display_name"] = df["user_display_name"].astype(str)
    df["user_name"] = df["user_name"].astype(str)
    df["position"] = df["position"].astype(str)
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
    df = df[["id", "text"]]
    df = df.rename(columns={"id": "object_id"})
    df = df.set_index(df["object_id"], verify_integrity=True)
    df["object_type"] = constants.OBJECT_TYPE_FACEBOOK_COMMENT
    return df
