"""YouTube videos data pull."""
from typing import Any, Dict, List, Optional

import json
import logging

import pandas as pd
import tentaclio

from phoenix.tag.data_pull import utils


YOUTUBE_VIDEOS_URL = "https://www.youtube.com/watch?v="
YOUTUBE_CHANNEL_URL = "https://www.youtube.com/channel/"


def execute(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Pull source json and create youtube_videos pre-tagging dataframe."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        if not utils.is_valid_file_name(entry):
            logging.info(f"Skipping file with invalid filename: {entry}")
            continue
        file_timestamp = utils.get_file_name_timestamp(entry)
        with tentaclio.open(entry) as file_io:
            js_obj = json.load(file_io)

        df = create_dataframe(js_obj)
        df["file_timestamp"] = file_timestamp
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
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
        df = create_dataframe_from_response(response)
        li.append(df)

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
