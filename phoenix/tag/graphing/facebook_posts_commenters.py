"""Processing and config for facebook_posts_commentors graph."""
from typing import Tuple

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry, processing_utilities


INPUT_DATASETS_ARTIFACT_KEYS = {
    "final_facebook_posts_classes": {
        "artifact_key": "final-facebook_posts_classes",
        "url_config_override": {},
    },
    "final_facebook_comments_inherited_accounts_classes": {
        "artifact_key": "final-objects_accounts_classes",
        "url_config_override": {"OBJECT_TYPE": "facebook_comments_inherited"},
    },
    "final_facebook_posts_objects_accounts_classes": {
        "artifact_key": "final-objects_accounts_classes",
        "url_config_override": {},
    },
}


def process_account_nodes(
    final_facebook_posts_objects_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process facebook accounts to create set of nodes of type `account`."""
    cols_to_keep = ["account_handle", "account_name", "account_label"]
    df = final_facebook_posts_objects_accounts_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["account_handle"], "account_label")
    df = df.rename(columns={"account_handle": "node_name", "account_name": "node_label"})
    df["type"] = "account"
    return df


def process_commenter_nodes(
    final_facebook_comments_inherited_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process facebook comments to create set of nodes of type `commenter`."""
    cols_to_keep = ["user_name", "user_display_name", "account_label"]
    df = final_facebook_comments_inherited_accounts_classes[cols_to_keep]
    df = df.dropna()
    df = processing_utilities.reduce_concat_classes(df, ["user_name"], "account_label")
    df = df.rename(columns={"user_name": "node_name", "user_display_name": "node_label"})
    df["type"] = "commenter"
    return df


def group_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Group the edges.

    Creating `class` `language_sentiment` and `post_count`.
    """
    df_classes = processing_utilities.reduce_concat_classes(
        df[["account_id_1", "account_id_2", "class"]], ["account_id_1", "account_id_2"], "class"
    )
    df_language_sentiment = processing_utilities.reduce_concat_classes(
        df[["account_id_1", "account_id_2", "language_sentiment"]],
        ["account_id_1", "account_id_2"],
        "language_sentiment",
    )
    df_class_language_sentiment = df_classes.merge(
        df_language_sentiment, on=["account_id_1", "account_id_2"], how="inner"
    )
    # Remove the class duplication
    df_size = df[["account_id_1", "account_id_2", "url_post_id"]]
    # DataFrame with row per post and user name
    df_size = df_size.drop_duplicates()
    df_size = (
        df_size.groupby(["account_id_1", "account_id_2"]).size().reset_index(name="post_count")
    )
    df = df_size.merge(
        df_class_language_sentiment, on=["account_id_1", "account_id_2"], validate="one_to_one"
    )
    return df


def process_account_commenter_edges(
    final_facebook_posts_classes: pd.DataFrame,
    final_facebook_comments_inherited_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process edges from accounts commenters."""
    facebook_comments = final_facebook_comments_inherited_accounts_classes[
        ["user_name", "url_post_id"]
    ]
    facebook_comments = facebook_comments.drop_duplicates()
    to_keep = ["account_handle", "url_post_id", "language_sentiment", "class"]
    facebook_posts_classes = final_facebook_posts_classes[to_keep]
    df = facebook_posts_classes.merge(facebook_comments, on="url_post_id")
    df["account_id_1"] = df["account_handle"]
    df["account_id_2"] = df["user_name"]
    return group_edges(df)


def process_commenter_commenter_edges(
    final_facebook_posts_classes: pd.DataFrame,
    final_facebook_comments_inherited_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process edges from accounts commenters."""
    facebook_posts_classes = final_facebook_posts_classes[
        ["url_post_id", "class", "language_sentiment"]
    ]
    # We don't need the class from the comments because they are inherited
    facebook_comments = final_facebook_comments_inherited_accounts_classes[
        ["user_name", "url_post_id"]
    ]
    facebook_comments = facebook_comments.drop_duplicates()
    df = facebook_posts_classes.merge(facebook_comments, on="url_post_id")
    df = df.rename(columns={"user_name": "account_id_1"})
    df = df.merge(facebook_comments, on="url_post_id")
    df["account_id_2"] = df["user_name"]
    df = df[df["account_id_1"] != df["account_id_2"]]
    df = group_edges(df)
    # Doing a deduplicate columns which contains the sorted account ids
    # to deduplicate the dataframe from have two vesion of the same commeter
    # connection but in both directions
    df_ids = df[["account_id_1", "account_id_2"]]
    df["deduplicate"] = [", ".join(sorted(filter(None, x))) for x in df_ids.to_numpy()]
    df = df.drop_duplicates(subset=["deduplicate"])
    df = df.drop(["deduplicate"], axis=1)
    return df


def process(
    final_facebook_posts_classes: pd.DataFrame,
    final_facebook_comments_inherited_accounts_classes: pd.DataFrame,
    final_facebook_posts_objects_accounts_classes: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process facebook accounts, posts, and commenters into three type network graph."""
    fciac_df = final_facebook_comments_inherited_accounts_classes
    fciac_df["url_post_id"] = fciac_df["post_id"].astype("string")
    # edges
    account_commenter_edges = process_account_commenter_edges(
        final_facebook_posts_classes, fciac_df
    )
    commenter_commenter_edges = process_commenter_commenter_edges(
        final_facebook_posts_classes, fciac_df
    )
    edges = account_commenter_edges.append(commenter_commenter_edges)

    # nodes
    account_nodes = process_account_nodes(final_facebook_posts_objects_accounts_classes)
    commenter_nodes = process_commenter_nodes(final_facebook_comments_inherited_accounts_classes)
    commenter_nodes = commenter_nodes[
        ~commenter_nodes["node_name"].isin(account_nodes["node_name"])
    ]
    nodes = account_nodes.append(commenter_nodes)
    nodes = nodes.reset_index(drop=True)

    edges = edges[edges["account_id_1"].isin(nodes["node_name"])]
    edges = edges[edges["account_id_2"].isin(nodes["node_name"])]
    edges = edges.reset_index(drop=True)

    return edges, nodes


plot_config = phoenix_graphistry.PlotConfig(
    edge_source_col="account_id_1",
    edge_destination_col="account_id_2",
    nodes_col="node_name",
    graph_name="facebook_commenters",
    graph_description="""
        Graph showing how commenters are connected.
        Nodes: commenters
        Edges: when commenters comment on the same posts.
    """,
    directed=False,
    color_by_type=False,
    node_label_col="node_label",
    edge_weight_col="post_count",
)
