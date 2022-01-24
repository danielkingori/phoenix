"""Processing and config for youtube_videos_commentors graph."""
import pandas as pd

from phoenix.tag.graphing import processing_utilities


INPUT_DATASETS_ARTIFACT_KEYS = [
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
