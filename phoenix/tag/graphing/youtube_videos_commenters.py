"""Processing and config for youtube_videos_commenters graph."""
from typing import Tuple

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry, processing_utilities


INPUT_DATASETS_ARTIFACT_KEYS = [
    "final-youtube_comments_classes",
    "final-youtube_videos_classes",
    "final-accounts",
]


def process_channel_nodes(final_accounts: pd.DataFrame) -> pd.DataFrame:
    """Process youtube accounts (channels) to create set of nodes of type `youtube_channel`."""
    cols_to_keep = ["object_user_name", "object_user_url", "account_label"]
    df = final_accounts[cols_to_keep]
    df["channel_id"] = df["object_user_url"].str.split("/").str[-1]
    df = processing_utilities.reduce_concat_classes(df, ["channel_id"], "account_label")
    df["node_name"] = df["channel_id"]
    df["type"] = "channel"
    df["node_label"] = df["object_user_name"]
    return df


def process_video_nodes(final_youtube_videos_classes: pd.DataFrame) -> pd.DataFrame:
    """Process youtube videos to create set of nodes of type `youtube_video`."""
    cols_to_keep = [
        "object_id",
        "title",
        "description",
        "video_url",
        "channel_title",
        "channel_id",
        "channel_url",
        "language_sentiment",
        "class",
    ]
    df = final_youtube_videos_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["object_id"], "class")
    df["node_name"] = df["object_id"]
    df["type"] = "video"
    df["node_label"] = df["title"]
    return df


def process_commenter_nodes(final_youtube_comments_classes: pd.DataFrame) -> pd.DataFrame:
    """Process youtube comments to create set of nodes of type `youtube_commenter`."""
    cols_to_keep = ["author_channel_id", "author_display_name", "class"]
    df = final_youtube_comments_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["author_channel_id"], "class")
    df["node_name"] = df["author_channel_id"]
    df["type"] = "commenter"
    df["node_label"] = df["author_display_name"]
    return df


def process_channel_video_edges(final_youtube_videos_classes: pd.DataFrame) -> pd.DataFrame:
    """Process edges from channels to videos."""
    df = final_youtube_videos_classes[["object_id", "channel_id"]]
    df = df.drop_duplicates()
    df["source_node"] = df["channel_id"]
    df["destination_node"] = df["object_id"]
    return df


def process_commenter_video_edges(final_youtube_comments_classes: pd.DataFrame) -> pd.DataFrame:
    """Process edges from commenters to videos."""
    # Filter for only top 1% of commenters
    commenter_comment_counts = final_youtube_comments_classes["author_channel_id"].value_counts()
    cut_off = commenter_comment_counts.quantile(0.99)
    filtered_commenter_comment_counts = commenter_comment_counts[
        commenter_comment_counts >= cut_off
    ]
    filtered_commenter_comment_counts
    final_youtube_comments_classes = final_youtube_comments_classes[
        final_youtube_comments_classes["author_channel_id"].isin(
            filtered_commenter_comment_counts.index
        )
    ]

    df = final_youtube_comments_classes[["video_id", "author_display_name", "author_channel_id"]]
    df = df.groupby(["video_id", "author_channel_id"]).size().reset_index()
    df = df.rename(columns={0: "times_commented"})
    df["source_node"] = df["author_channel_id"]
    df["destination_node"] = df["video_id"]
    return df


def process(
    final_youtube_comments_classes: pd.DataFrame,
    final_youtube_videos_classes: pd.DataFrame,
    final_accounts: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process youtube channels, videos, and commenters into three type network graph."""
    # edges
    channel_videos_edges = process_channel_video_edges(final_youtube_videos_classes)
    commenter_videos_edges = process_commenter_video_edges(final_youtube_comments_classes)
    channel_videos_edges = channel_videos_edges[
        channel_videos_edges["object_id"].isin(commenter_videos_edges["video_id"])
    ]
    edges = channel_videos_edges.append(commenter_videos_edges)

    # nodes
    channel_nodes = process_channel_nodes(final_accounts)
    channel_nodes = channel_nodes[channel_nodes["channel_id"].isin(edges["channel_id"])]
    video_nodes = process_video_nodes(final_youtube_videos_classes)
    video_nodes = video_nodes[video_nodes["object_id"].isin(edges["video_id"])]
    commenter_nodes = process_commenter_nodes(final_youtube_comments_classes)
    commenter_nodes = commenter_nodes[
        commenter_nodes["author_channel_id"].isin(edges["author_channel_id"])
    ]
    nodes = channel_nodes.append(video_nodes).append(commenter_nodes)
    nodes = nodes.reset_index(drop=True)

    edges = edges[edges["source_node"].isin(nodes["node_name"])]
    edges = edges[edges["destination_node"].isin(nodes["node_name"])]
    edges = edges.reset_index(drop=True)

    edges, nodes = processing_utilities.account_post_commenter_graph_to_commenter_edges(
        edges, nodes
    )

    return edges, nodes


plot_config = phoenix_graphistry.PlotConfig(
    edge_source_col="source_node",
    edge_destination_col="destination_node",
    nodes_col="node_name",
    graph_name="youtube_commenters",
    graph_description="""
        Graph showing how commenters are connected.
        Nodes: commenters
        Edges: when commenters comment on the same posts.
    """,
    directed=False,
    color_by_type=False,
    node_label_col="node_label",
    edge_weight_col="joint_num_comments",
)
