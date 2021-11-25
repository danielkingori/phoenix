"""YouTube comments data pull (i.e. processing of raw scraped data.

Specifically this processing raw json data of kind `youtube#commentThreadListResponse`.
"""
from typing import Any, Dict, List, Optional

import pandas as pd

from phoenix.tag.data_pull import utils


def from_json(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Process source json into youtube_comments pre-tagging dataframe."""
    json_objects = utils.get_jsons(url_to_folder)

    dfs: List[pd.DataFrame] = []
    for json_object, file_timestamp in json_objects:
        for comment_thread_list in json_object:
            if comment_thread_list["items"]:
                df = process_comment_thread_list_json(comment_thread_list)
                df["file_timestamp"] = file_timestamp
                dfs.append(df)

    df = utils.concat_dedupe_sort_objects(dfs, "published_at")
    df = df[
        [
            "id",
            "published_at",
            "updated_at",
            "text",
            "text_original",
            "like_count",
            "is_top_level_comment",
            "total_reply_count",
            "parent_comment_id",
            "author_channel_id",
            "author_display_name",
            "channel_id",
            "video_id",
            "etag",
            "response_etag",
            "timestamp_filter",
            "date_filter",
            "year_filter",
            "month_filter",
            "day_filter",
            "file_timestamp",
        ]
    ]
    df = utils.filter_df(df, year_filter, month_filter)
    return df


def process_comment_thread_list_json(comment_thread_list: Dict[str, Any]) -> pd.DataFrame:
    """Process a youtube#commentThreadListResponse json."""
    dfs = []
    for comment_thread in comment_thread_list["items"]:
        dfs.append(process_comment_thread_json(comment_thread))
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = utils.add_filter_cols(df, df["published_at"])
    df["response_etag"] = comment_thread_list["etag"]
    return df


def process_comment_thread_json(comment_thread_json: Dict[str, Any]) -> pd.DataFrame:
    """Process a youtube#commentThread json."""
    df = process_comments_json([comment_thread_json["snippet"]["topLevelComment"]])
    df["is_top_level_comment"] = True
    df["total_reply_count"] = comment_thread_json["snippet"]["totalReplyCount"]
    df["parent_comment_id"] = None

    if "replies" in comment_thread_json and comment_thread_json["replies"]["comments"]:
        replies_df = process_comments_json(comment_thread_json["replies"]["comments"])
        replies_df["is_top_level_comment"] = False
        replies_df["total_reply_count"] = None
        replies_df["parent_comment_id"] = df.iloc[0]["id"]
        df = pd.concat([df, replies_df], axis=0, ignore_index=True)
    return df


def process_comments_json(comments_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process youtube#comment(s) in replies."""
    df = pd.json_normalize(comments_list)
    df = df.rename(
        columns={
            "snippet.publishedAt": "published_at",
            "snippet.updatedAt": "updated_at",
            "snippet.textDisplay": "text",
            "snippet.textOriginal": "text_original",
            "snippet.likeCount": "like_count",
            "snippet.channelId": "channel_id",
            "snippet.videoId": "video_id",
            "snippet.authorChannelId.value": "author_channel_id",
            "snippet.authorDisplayName": "author_display_name",
        },
    )
    df = df[
        [
            "id",
            "published_at",
            "updated_at",
            "text",
            "text_original",
            "like_count",
            "author_channel_id",
            "author_display_name",
            "channel_id",
            "video_id",
            "etag",
        ]
    ]
    for col in ["published_at", "updated_at"]:
        df[col] = pd.to_datetime(df[col])
    return df


def for_tagging(given_df: pd.DataFrame):
    """Get YouTube comments for tagging.

    Return:
    dataframe  : pandas.DataFrame
    Index:
        object_id: String, dtype: string
    Columns:
        object_id: String, dtype: string
        text: String, dtype: string
        object_type: "facebook_post", dtype: String
        created_at: datetime
        object_url: String, dtype: string
        object_user_url: String, dtype: string
        object_user_name: String, dtype: string
    """
    pass
