"""Channels functionality."""
from typing import Any, List, Optional

import pandas as pd
from googleapiclient import discovery

from phoenix.scrape.youtube import lists, utils


DEFAULT_PARTS_TO_REQUEST = [
    "brandingSettings",
    "contentDetails",
    "contentOwnerDetails",
    "id",
    "localizations",
    "snippet",
    "statistics",
    "status",
]


RESOURCE_CLIENT = "channels"


def get_channels(
    channels_config: pd.DataFrame,
    parts_list: List[str] = DEFAULT_PARTS_TO_REQUEST,
    max_pages: int = 10000,
    client: Optional[discovery.Resource] = None,
) -> List[Any]:
    """Get all the channels data from the channels_config.

    Using:
    https://developers.google.com/resources/api-libraries/documentation/youtube/v3/python/latest/youtube_v3.channels.html

    Arguments:
        channels_config (DataFrame): A channels config dataframe see: ./channels_config.py
        parts_list (List[str]): A list of parts that should be requested.
            See: https://developers.google.com/youtube/v3/docs/channels/list#part
        max_pages (int): Maximum number of pages (and thus API quota usage) to request.
        client (discovery.Resource): YouTube client to override the default

    Returns:
        List of dictionaries that contain the channel list resource.
    """
    channels = utils.get_resource_client(RESOURCE_CLIENT, client)
    part_str = utils.get_part_str(parts_list)
    channel_ids_str = _get_channel_ids_str(channels_config)
    request = channels.list(part=part_str, id=channel_ids_str, maxResults=50)
    return lists.paginate_list_resource(channels, request, max_pages=max_pages)


def _get_channel_ids_str(channels_config: pd.DataFrame):
    """Get the channel_ids_str."""
    return ",".join(channels_config["channel_id"].values)
