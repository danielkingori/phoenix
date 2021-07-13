"""Data pulling for facebook comments."""
import itertools
import json

import pandas as pd
import tentaclio

from phoenix.tag.data_pull import constants


def from_json(url_to_folder: str) -> pd.DataFrame:
    """Get all the jsons and return a normalised facebook comments."""
    comment_li = []
    for entry in tentaclio.listdir(url_to_folder):
        with tentaclio.open(entry) as file_io:
            pages = json.load(file_io)
            comments_df = get_comments_df(pages)
            comment_li.append(comments_df)

    df = pd.concat(comment_li, axis=0, ignore_index=True)
    df = df.groupby("id").last()
    df = df.reset_index()
    return normalise_comments_dataframe(df)


def get_comments_df(pages):
    """Get the comments dataframe from pages."""
    comments = list(itertools.chain.from_iterable([get_comments(page) for page in pages]))
    return pd.DataFrame(comments)


def get_comments(page_json):
    """Get comments from page."""
    page = json.loads(page_json)
    return [normalise_comment(comment, page) for comment in page["comments"]]


def normalise_comment(comment, page):
    """Normalise_comment."""
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
        "username": comment["username"],
        "position": comment["position"],
    }


def normalise_comments_dataframe(df):
    """Normalise the comments data frame."""
    df["id"] = df["id"].astype(int)
    df["post_id"] = df["post_id"].astype(int)
    df["file_id"] = df["file_id"].astype(str)
    df["parent_id"] = df["parent_id"].astype(int)
    df["post_created"] = pd.to_datetime(df["post_created"]).dt.tz_localize("UTC")
    df["text"] = df["text"].astype(str)
    df["reactions"] = df["reactions"].astype(int)
    df["user_display_name"] = df["user_display_name"].astype(str)
    df["username"] = df["username"].astype(str)
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
