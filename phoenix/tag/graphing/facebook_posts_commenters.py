"""Processing and config for facebook_posts_commentors graph."""
from typing import Optional, Tuple

import logging

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry, processing_utilities


INPUT_DATASETS_ARTIFACT_KEYS = {
    "tagging_runs_facebook_posts_classes_final": {
        "artifact_key": "tagging_runs-facebook_posts_classes_final",
        "url_config_override": {},
    },
    "tagging_runs_facebook_comments_classes_final": {
        "artifact_key": "tagging_runs-objects_accounts_classes_final",
        "url_config_override": {"OBJECT_TYPE": "facebook_comments"},
    },
    "tagging_runs_facebook_posts_objects_accounts_classes_final": {
        "artifact_key": "tagging_runs-objects_accounts_classes_final",
        "url_config_override": {},
    },
}

SIZE_COLUMN_NAME = "post_count"


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
    return processing_utilities.commenters_group_edges(df, "url_post_id", SIZE_COLUMN_NAME)


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
    df = processing_utilities.commenters_group_edges(df, "url_post_id", SIZE_COLUMN_NAME)
    # Doing a deduplicate columns which contains the sorted account ids
    # to deduplicate the dataframe from have two vesion of the same commeter
    # connection but in both directions
    df_ids = df[["account_id_1", "account_id_2"]]
    df["deduplicate"] = [", ".join(sorted(filter(None, x))) for x in df_ids.to_numpy()]
    df = df.drop_duplicates(subset=["deduplicate"])
    df = df.drop(["deduplicate"], axis=1)
    return df


def get_subset_of_commenters(
    final_facebook_comments_inherited_accounts_classes: pd.DataFrame,
    limit_top_commenters: Optional[int] = None,
    quantile_of_commenters=None,
) -> pd.DataFrame:
    """Limit the number of comments."""
    fciac_df = final_facebook_comments_inherited_accounts_classes
    fciac_df = fciac_df.sort_values(by=["user_name", "url_post_id"])
    commenter_comment_counts = (
        fciac_df[["user_name", "url_post_id"]].drop_duplicates()["user_name"].value_counts()
    )
    commenter_comment_counts = commenter_comment_counts.reset_index(drop=False)
    # value_count messes up the names
    commenter_comment_counts = commenter_comment_counts.rename(
        columns={"user_name": "comment_count", "index": "commenter_name"}
    )
    commenter_comment_counts = commenter_comment_counts.sort_values(
        by=["comment_count", "commenter_name"], ascending=[False, False]
    )
    if quantile_of_commenters:
        cut_off = commenter_comment_counts["comment_count"].quantile(quantile_of_commenters)
        commenter_comment_counts = commenter_comment_counts[
            commenter_comment_counts["comment_count"] >= cut_off
        ]

    if limit_top_commenters and limit_top_commenters < commenter_comment_counts.shape[0]:
        commenter_comment_counts = commenter_comment_counts[:limit_top_commenters]

    number_of_commenters = commenter_comment_counts.shape[0]

    logging.info(f"Commenters have been filtered to include a subset of: {number_of_commenters}.")
    return fciac_df[fciac_df["user_name"].isin(commenter_comment_counts["commenter_name"])]


def process(
    final_facebook_posts_classes: pd.DataFrame,
    final_facebook_comments_inherited_accounts_classes: pd.DataFrame,
    final_facebook_posts_objects_accounts_classes: pd.DataFrame,
    limit_top_commenters: Optional[int] = 2000,
    quantile_of_commenters=None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process facebook accounts, posts, and commenters into three type network graph.

    Args:
        final_facebook_posts_classes: DataFrame of the facebook posts classes.
        final_facebook_comments_inherited_accounts_classes: DataFrame of the facebook comments with
            the inherited classes.
        final_facebook_posts_objects_accounts_classes: DataFrame of the facebook posts
            and account classes
        limit_top_commenters: limit the number of commenters that are used to compute graph.
            Default is 2000 which seems to be a good amount: not to long to compute and graphistry
            can handle this amount.
        quantile_of_commenters: If you want to filter commenters by a quantile of
            the most active commenters.

    Returns:
        edges dataframe, nodes dataframe
    """
    fciac_df = final_facebook_comments_inherited_accounts_classes
    fciac_df["url_post_id"] = fciac_df["post_id"].astype("string")
    fciac_df = get_subset_of_commenters(fciac_df, limit_top_commenters, quantile_of_commenters)
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
    commenter_nodes = process_commenter_nodes(fciac_df)
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
