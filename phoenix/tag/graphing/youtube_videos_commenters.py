"""Processing and config for youtube_videos_commentors graph."""
import pandas as pd

from phoenix.tag.graphing import processing_utilities


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
    df["type"] = "youtube_channel"
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
    df["type"] = "youtube_video"
    return df


def process_commenter_nodes(final_youtube_comments_classes: pd.DataFrame) -> pd.DataFrame:
    """Process youtube comments to create set of nodes of type `youtube_commenter`."""
    cols_to_keep = ["author_channel_id", "author_display_name", "class"]
    df = final_youtube_comments_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["author_channel_id"], "class")
    df["node_name"] = df["author_channel_id"]
    df["type"] = "youtube_commenter"
    return df


def process_channel_video_edges(final_youtube_videos_classes: pd.DataFrame) -> pd.DataFrame:
    """Process edges from channels to videos."""
    df = final_youtube_videos_classes[["object_id", "channel_id"]]
    df = df.drop_duplicates()
    df["source_node"] = df["channel_id"]
    df["destination_node"] = df["object_id"]
    return df
