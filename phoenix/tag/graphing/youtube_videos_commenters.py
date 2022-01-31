"""Processing and config for youtube_videos_commenters graph."""
from typing import Tuple

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry, processing_utilities


INPUT_DATASETS_ARTIFACT_KEYS = {
    "final_youtube_videos_classes": {
        "artifact_key": "final-youtube_videos_classes",
        "url_config_override": {},
    },
    "final_youtube_comments_objects_accounts_classes": {
        "artifact_key": "final-objects_accounts_classes",
        "url_config_override": {"OBJECT_TYPE": "youtube_comments"},
    },
    "final_youtube_videos_objects_accounts_classes": {
        "artifact_key": "final-objects_accounts_classes",
        "url_config_override": {},
    },
}

SIZE_COLUMN_NAME = "video_count"


def process_channel_nodes(
    final_youtube_videos_objects_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process youtube accounts (channels) to create set of nodes of type `youtube_channel`."""
    cols_to_keep = ["channel_id", "channel_title", "account_label"]
    df = final_youtube_videos_objects_accounts_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["channel_id"], "account_label")
    df = df.rename(columns={"channel_id": "node_name", "channel_title": "node_label"})
    df["type"] = "channel"
    return df


def process_commenter_nodes(
    final_youtube_comments_objects_accounts_classes: pd.DataFrame,
    final_youtube_videos_objects_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process youtube comments to create set of nodes of type `youtube_commenter`."""
    inheritable_labels = final_youtube_videos_objects_accounts_classes[["id", "account_label"]]
    to_join = final_youtube_comments_objects_accounts_classes[["author_channel_id", "video_id"]]
    to_join = to_join.rename(columns={"video_id": "id"})
    inherited_classes_df = to_join.merge(inheritable_labels, on="id", how="inner")
    inherited_classes_df = inherited_classes_df.drop("id", axis=1)
    cols_to_keep = ["author_channel_id", "author_display_name", "account_label"]
    df = final_youtube_comments_objects_accounts_classes[cols_to_keep]
    df = df.append(inherited_classes_df)
    df = processing_utilities.reduce_concat_classes(df, ["author_channel_id"], "account_label")
    df = df.rename(columns={"author_channel_id": "node_name", "author_display_name": "node_label"})
    df["type"] = "commenter"
    return df


def process_channel_video_edges(
    final_youtube_videos_classes: pd.DataFrame,
    final_youtube_comments_objects_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process edges from channels to videos."""
    comments = final_youtube_comments_objects_accounts_classes[["author_channel_id", "video_id"]]
    comments = comments.rename(columns={"video_id": "id"})
    comments = comments.drop_duplicates()
    to_keep = ["channel_id", "id", "language_sentiment", "class"]
    classes = final_youtube_videos_classes[to_keep]
    df = classes.merge(comments, on="id")
    df["account_id_1"] = df["channel_id"]
    df["account_id_2"] = df["author_channel_id"]
    return processing_utilities.commenters_group_edges(df, "id", SIZE_COLUMN_NAME)


def process_commenter_video_edges(
    final_youtube_videos_classes: pd.DataFrame,
    final_youtube_comments_objects_accounts_classes: pd.DataFrame,
) -> pd.DataFrame:
    """Process edges from commenters to videos."""
    # Filter for only top 1% of commenters
    classes = final_youtube_videos_classes[["id", "class", "language_sentiment"]]
    # We don't need the class from the comments because they are inherited
    comments = final_youtube_comments_objects_accounts_classes[["author_channel_id", "video_id"]]
    comments = comments.rename(columns={"video_id": "id"})
    comments = comments.drop_duplicates()
    df = classes.merge(comments, on="id")
    df = df.rename(columns={"author_channel_id": "account_id_1"})
    df = df.merge(comments, on="id")
    df["account_id_2"] = df["author_channel_id"]
    df = df[df["account_id_1"] != df["account_id_2"]]
    df = processing_utilities.commenters_group_edges(df, "id", SIZE_COLUMN_NAME)
    # Doing a deduplicate columns which contains the sorted account ids
    # to deduplicate the dataframe from have two vesion of the same commeter
    # connection but in both directions
    df_ids = df[["account_id_1", "account_id_2"]]
    df["deduplicate"] = [", ".join(sorted(filter(None, x))) for x in df_ids.to_numpy()]
    df = df.drop_duplicates(subset=["deduplicate"])
    df = df.drop(["deduplicate"], axis=1)
    return df


def process(
    final_youtube_videos_classes: pd.DataFrame,
    final_youtube_comments_objects_accounts_classes: pd.DataFrame,
    final_youtube_videos_objects_accounts_classes: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process youtube channels, videos, and commenters into network graph."""
    # edges
    channel_videos_edges = process_channel_video_edges(
        final_youtube_videos_classes,
        final_youtube_comments_objects_accounts_classes,
    )
    commenter_videos_edges = process_commenter_video_edges(
        final_youtube_videos_classes,
        final_youtube_comments_objects_accounts_classes,
    )
    edges = channel_videos_edges.append(commenter_videos_edges)

    # nodes
    account_nodes = process_channel_nodes(final_youtube_videos_objects_accounts_classes)
    commenter_nodes = process_commenter_nodes(
        final_youtube_comments_objects_accounts_classes,
        final_youtube_videos_objects_accounts_classes,
    )
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
    graph_name="youtube_commenters",
    graph_description="""
        Graph showing how commenters are connected.
        Nodes: commenters
        Edges: when commenters comment on the same posts.
    """,
    directed=False,
    color_by_type=False,
    node_label_col="node_label",
    edge_weight_col="video_count",
)
