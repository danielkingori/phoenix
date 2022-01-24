"""Processing and config for facebook_posts_commentors graph."""
from typing import Tuple

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
    df["node_name"] = df["object_user_name"]
    df["type"] = "facebook_account"
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
    df["node_name"] = df["object_id"]
    df["type"] = "facebook_post"
    return df


def process_commenter_nodes(final_facebook_comments_classes: pd.DataFrame) -> pd.DataFrame:
    """Process facebook comments to create set of nodes of type `commenter`."""
    cols_to_keep = ["user_name", "class"]
    df = final_facebook_comments_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["user_name"], "class")
    df["node_name"] = df["user_name"]
    df["type"] = "facebook_commenter"
    return df


def process_account_post_edges(final_facebook_posts_classes: pd.DataFrame) -> pd.DataFrame:
    """Process edges from accounts to posts."""
    df = final_facebook_posts_classes[["object_id", "account_handle"]]
    df = df.drop_duplicates()
    df["source_node"] = df["account_handle"]
    df["destination_node"] = df["object_id"]
    return df


def process_commenter_post_edges(final_facebook_comments_classes: pd.DataFrame) -> pd.DataFrame:
    """Process edges from commenters to posts."""
    df = final_facebook_comments_classes[["post_id", "user_name"]]
    df = df.groupby(["post_id", "user_name"]).size().reset_index()
    df = df.rename(columns={0: "times_commented"})
    df["source_node"] = df["user_name"]
    df["destination_node"] = df["post_id"]
    return df


def process(
    final_facebook_comments_classes: pd.DataFrame,
    final_facebook_posts_classes: pd.DataFrame,
    final_accounts: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process facebook accounts, posts, and commenters into three type network graph."""
    # edges
    account_post_edges = process_account_post_edges(final_facebook_posts_classes)
    commenter_post_edges = process_commenter_post_edges(final_facebook_comments_classes)
    edges = account_post_edges.append(commenter_post_edges)

    # nodes
    account_nodes = process_account_nodes(final_accounts)
    post_nodes = process_post_nodes(final_facebook_posts_classes)
    commenter_nodes = process_commenter_nodes(final_facebook_comments_classes)
    nodes = account_nodes.append(post_nodes).append(commenter_nodes)

    return edges, nodes
