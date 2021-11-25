"""YouTube videos data pull."""
from typing import Any, Dict, List, Optional

import logging

import pandas as pd

from phoenix.tag.data_pull import constants, utils


logger = logging.getLogger(__name__)


JSONType = Any


YOUTUBE_VIDEOS_URL = constants.YOUTUBE_VIDEOS_URL
YOUTUBE_CHANNEL_URL = constants.YOUTUBE_CHANNEL_URL


def from_json(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Pull source json and create youtube_videos pre-tagging dataframe."""
    json_objects = utils.get_jsons(url_to_folder)

    dfs: List[pd.DataFrame] = []
    for json_object, file_timestamp in json_objects:
        df = create_dataframe(json_object)
        df["file_timestamp"] = file_timestamp
        dfs.append(df)

    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = df.sort_values("file_timestamp")
    df = df.groupby("id").last()
    df = df.reset_index()
    df = df.sort_values("created_at", ascending=False).reset_index(drop=True)
    if year_filter:
        df = df[df["year_filter"] == year_filter]
    if month_filter:
        df = df[df["month_filter"] == month_filter]
    return df


def create_dataframe(json_obj: List[Dict[str, Any]]) -> pd.DataFrame:
    """Create Dataframe from the raw json."""
    # raw json contains a list of responses
    li = []
    for response in json_obj:
        if response["items"]:
            df = create_dataframe_from_response(response)
            li.append(df)
        else:
            logger.info(f"No videos found for etag: [etag={response['etag']}]")

    return pd.concat(li, axis=0, ignore_index=True)


def create_dataframe_from_response(response: Dict[str, Any]) -> pd.DataFrame:
    """Create youtube videos dataframe from response."""
    response_etag = response["etag"]
    df = pd.json_normalize(response["items"])
    df = df.rename(
        columns={
            "id.videoId": "id",
            "snippet.publishedAt": "created_at",
            "snippet.title": "title",
            "snippet.description": "description",
            "snippet.channelId": "channel_id",
            "snippet.channelTitle": "channel_title",
        },
    )
    df["text"] = df["title"] + " " + df["description"]
    df["video_url"] = YOUTUBE_VIDEOS_URL + df["id"]
    df["channel_url"] = YOUTUBE_CHANNEL_URL + df["channel_id"]
    df = df[
        [
            "id",
            "created_at",
            "channel_id",
            "channel_title",
            "title",
            "description",
            "text",
            "video_url",
            "channel_url",
            "etag",
        ]
    ]
    df["response_etag"] = response_etag
    df["created_at"] = pd.to_datetime(df["created_at"])
    return utils.add_filter_cols(df, df["created_at"])


def for_tagging(given_df: pd.DataFrame):
    """Get YouTube videos for tagging.

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
    df = given_df.copy()
    df = df.rename(columns={"id": "object_id"})
    df["object_type"] = constants.OBJECT_TYPE_YOUTUBE_VIDEO
    df["object_url"] = df["video_url"]
    df["object_user_url"] = df["channel_url"]
    df["object_user_name"] = df["channel_title"]
    df = df[
        [
            "object_id",
            "text",
            "object_type",
            "created_at",
            "object_url",
            "object_user_url",
            "object_user_name",
        ]
    ]
    df = df.set_index("object_id", drop=False, verify_integrity=True)
    return df
