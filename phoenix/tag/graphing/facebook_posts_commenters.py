"""Processing and config for facebook_posts_commentors graph."""

import pandas as pd

from phoenix.tag.graphing import processing_utilities


ARTIFACT_KEYS = [
    "final-facebook_comments_classes",
    "final-facebook_posts_classes",
    "final-accounts",
]


def process_account_nodes(final_accounts: pd.DataFrame) -> pd.DataFrame:
    """Process facebook accounts to create set of nodes of type `account`."""
    cols_to_keep = ["object_user_name", "object_user_url", "account_label"]
    df = final_accounts[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["object_user_name"], "account_label")
    return df


def process_post_nodes(final_facebook_posts_classes: pd.DataFrame) -> pd.DataFrame:
    """Process facebook posts to create set of nodes of type `post`."""
    cols_to_keep = [
        "object_id",
        "platform_id",
        "account_handle",
        "account_platform_id",
        "medium_type",
        "text",
        "class",
    ]
    cols_to_keep = cols_to_keep + [
        col for col in final_facebook_posts_classes.columns if "statistics" in col
    ]
    df = final_facebook_posts_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["object_id"], "class")
    return df


def process_commenter_nodes(final_facebook_comments_classes: pd.DataFrame) -> pd.DataFrame:
    """Process facebook comments to create set of nodes of type `commenter`."""
    cols_to_keep = ["user_name", "class"]
    df = final_facebook_comments_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["user_name"], "class")
    return df


def process_account_post_edges(final_facebook_posts_classes: pd.DataFrame) -> pd.DataFrame:
    """Process edges from accounts to posts."""
    df = final_facebook_posts_classes[["object_id", "account_handle"]]
    df = df.drop_duplicates()
    return df
