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
    parts_list: Optional[List[str]] = None,
    client: Optional[discovery.Resource] = None,
) -> List[Any]:
    """Get all the channels data from the channels_config.

    Using:
    https://developers.google.com/resources/api-libraries/documentation/youtube/v3/python/latest/youtube_v3.channels.html

    Arguments:
        channels_config (DataFrame): A channels config dataframe see: ./channels_config.py
        parts_list: An optional list of parts that should be requested. Default is None.
            If None then DEFAULT_PARTS_TO_REQUEST is used.
            See:
            https://developers.google.com/youtube/v3/docs/channels/list#part
        client: YouTube client to override the default

    Returns:
        List of dictionaries that contain the channel list resource.
    """
    channels = utils.get_resource_client(RESOURCE_CLIENT, client)
    part_str = _get_part_str(parts_list)
    channel_ids_str = _get_channel_ids_str(channels_config)
    request = channels.list(part=part_str, id=channel_ids_str)
    return lists.paginate_list_resource(channels, request)


def _get_channel_ids_str(channels_config: pd.DataFrame):
    """Get the channel_ids_str."""
    return ",".join(channels_config["channel_id"].values)


def _get_part_str(
    parts_list: Optional[List[str]] = None,
) -> str:
    """Get the part string for the request."""
    return utils.get_part_str(DEFAULT_PARTS_TO_REQUEST, parts_list)