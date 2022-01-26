"""YouTube comments data pull (i.e. processing of raw scraped data.

Specifically this processing raw json data of kind `youtube#commentThreadListResponse`.
"""
from typing import Any, Dict, List, Optional

import pandas as pd

from phoenix.tag.data_pull import constants, utils


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
    df["video_url"] = constants.YOUTUBE_VIDEOS_URL + df["video_id"]
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
            "video_url",
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
    """Process youtube#comment(s) in replies.

    We use `snippet.textDisplay` as `text`, as `snippet.textOriginal` is not always guaranteed to
    be available in the response.
    """
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
    # This data is not always guaranteed to be present, so fill with NaN if not in response.
    if "author_channel_id" not in df:
        df["author_channel_id"] = None
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
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def for_tagging(given_df: pd.DataFrame):
    """Get YouTube comments for tagging.

    Return:
    dataframe  : pandas.DataFrame
    Index:
        object_id: ID of object from source system/platform, dtype: string
    Columns:
        object_id: ID of object from source system/platform, dtype: string
        text: Text content of the object, dtype: string
        object_type: Type i.e. class of the object, dtype: String
        created_at: Date time when object was created/posted dtype: datetime
        object_url: URL to location of object, dtype: string
        object_user_url: URL to creator/author of object, if available dtype: Optional[string]
        object_user_name: User name of creator/author, dtype: string
        object_parent_text: Text of the parent object, if applicable, dtype: Optional[string]
    """
    df = given_df.copy()
    df = df.merge(
        df[["id", "text"]].rename(columns={"id": "_id", "text": "parent_comment_text"}),
        how="left",
        left_on="parent_comment_id",
        right_on="_id",
    )
    df = df.rename(
        columns={
            "id": "object_id",
            "published_at": "created_at",
            "parent_comment_text": "object_parent_text",
        }
    )
    df["object_type"] = constants.OBJECT_TYPE_YOUTUBE_COMMENT
    df["object_url"] = df["video_url"]
    df["object_user_url"] = constants.YOUTUBE_CHANNEL_URL + df["author_channel_id"]
    df["object_user_name"] = df["author_display_name"]
    df = df[
        [
            "object_id",
            "text",
            "object_type",
            "created_at",
            "object_url",
            "object_user_url",
            "object_user_name",
            "object_parent_text",
        ]
    ]
    df = df.set_index("object_id", drop=False, verify_integrity=True)
    return df
