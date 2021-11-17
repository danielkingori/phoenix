"""Scraping comment threads.

A comment thread is all the comments for a video or channel.

Using the `allThreadsRelatedToChannelId` API parameter, one can retrieve a response that includes
all comments threads; about the channel and about the channel's videos.
"""
from typing import List, Optional

import logging

import pandas as pd
from googleapiclient import discovery

from phoenix.scrape.youtube import lists, utils


logger = logging.getLogger(__name__)

DEFAULT_PARTS_TO_REQUEST = [
    "id",
    "snippet",
    "replies",
]


def get_comment_threads(
    channel_id: str,
    parts_list: List[str] = DEFAULT_PARTS_TO_REQUEST,
    client: Optional[discovery.Resource] = None,
) -> lists.ListResults:
    """Get all the comment threads data for channel id.

    Using:
    https://developers.google.com/resources/api-libraries/documentation/youtube/v3/python/latest/youtube_v3.commentThreads.html

    Arguments:
        channels_id (str): Channel id to get the comment threads for.
        parts_list (List[str]): A list of parts that should be requested.
            See: https://developers.google.com/youtube/v3/docs/commentThreads/list#part
        client (discovery.Resource): YouTube client. If None then one will be initialised.

    Returns:
        List of dictionaries that contain the commentThread resource.
    """
    comment_threads_client = utils.get_resource_client("commentThreads", client)
    part_str = utils.get_part_str(parts_list)
    request = comment_threads_client.list(
        part=part_str,
        allThreadsRelatedToChannelId=channel_id,
        maxResults=100,
        moderationStatus="published",
        order="time",
        textFormat="plainText",
    )
    return lists.paginate_list_resource(comment_threads_client, request)


def get_comment_threads_for_channel_config(
    channels_config: pd.DataFrame,
) -> lists.ListResults:
    """Get all the comment threads data for all the channels specified in the config."""
    result: lists.ListResults = []
    for channel_id in channels_config["channel_id"].values:
        logger.info(f"Scraping comment threads for [channel_id={channel_id}]")
        found_results = get_comment_threads(channel_id)
        result = result + found_results
    return result
