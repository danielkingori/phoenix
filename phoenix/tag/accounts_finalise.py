"""Finalise accounts, and text snippets (objects) with account classes (labels) joined to them."""
from typing import Dict

import pandas as pd

from phoenix.tag.data_pull import constants


def accounts(object_type: str, accounts_df: pd.DataFrame) -> pd.DataFrame:
    """Process accounts for object type.

    Currently is just a pass through.
    """
    return accounts_df


def objects_accounts_classes(
    object_type: str, objects_df: pd.DataFrame, accounts_df: pd.DataFrame
) -> pd.DataFrame:
    """Join accounts classes onto their corresponding text snippets (objects)."""
    accounts_df = accounts_df.copy()
    left_on_mapping: Dict[str, str] = {
        "facebook_posts": "account_url",
        "youtube_videos": "channel_url",
        "youtube_comments": "author_channel_id",
        "tweets": "user_screen_name",
    }
    if object_type == "youtube_comments":
        accounts_df["object_user_url"] = accounts_df["object_user_url"].str.replace(
            constants.YOUTUBE_CHANNEL_URL, ""
        )
    elif object_type == "tweets":
        accounts_df["object_user_url"] = accounts_df["object_user_url"].str.replace(
            constants.TWITTER_URL, ""
        )
    df = objects_df.merge(
        accounts_df[["object_user_url", "account_label"]],
        how="inner",
        left_on=left_on_mapping[object_type],
        right_on="object_user_url",
    )
    df = df.drop(columns="object_user_url")
    return df
