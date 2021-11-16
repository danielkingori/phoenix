"""Search functionality."""
from typing import Any, List, Optional

import datetime

import pandas as pd
from googleapiclient import discovery

from phoenix.scrape.youtube import lists, utils


DEFAULT_PARTS_TO_REQUEST = ["id", "snippet"]
DEFAULT_ORDER = "date"

TYPE_VIDEO = "video"

RESOURCE_CLIENT = "search"


def get_videos_for_channel(
    channel_id: str,
    published_after: datetime.datetime,
    parts_list: Optional[List[str]] = None,
    order: Optional[str] = None,
    client: Optional[discovery.Resource] = None,
) -> List[Any]:
    """Get all the videos data for a channel.

    Using:
    https://developers.google.com/resources/api-libraries/documentation/youtube/v3/python/latest/youtube_v3.search.html

    Arguments:
        channel_id (str): Channel id to get the videos for
        published_after (datetime): a UTC datetime to get videos after
        parts_list: An optional list of parts that should be requested. Default is None.
            If None then DEFAULT_PARTS_TO_REQUEST is used.
            See:
            https://developers.google.com/youtube/v3/docs/search/list#part
        order: Order of the videos
        client: YouTube client to override the default

    Returns:
        List of dictionaries that contain the channel list resource.
    """
    if not order:
        order = DEFAULT_ORDER
    resource_client = utils.get_resource_client(RESOURCE_CLIENT, client)
    part_str = utils.get_part_str(DEFAULT_PARTS_TO_REQUEST, parts_list)
    published_after_str = utils.datetime_str(published_after)
    request = resource_client.list(
        part=part_str,
        channelId=channel_id,
        publishedAfter=published_after_str,
        order=order,
        type=TYPE_VIDEO,
    )
    return lists.paginate_list_resource(resource_client, request)


def get_videos_for_channel_config(
    channels_config: pd.DataFrame,
    published_after: datetime.datetime,
):
    """Get the videos for a channel config."""
    result: lists.ListResults = []
    for channel_id in channels_config["channel_id"].values:
        found_results = get_videos_for_channel(channel_id, published_after)
        result = result + found_results
    return result
