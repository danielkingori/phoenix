"""Channels functionality."""
from typing import Any, Dict, List, Optional

import pandas as pd
from googleapiclient import discovery

from phoenix.scrape.youtube import utils


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


def get_channels(
    cur_chanels_config: pd.DataFrame,
    parts_list: Optional[List[str]] = None,
    client: Optional[discovery.Resource] = None,
) -> List[Any]:
    """Get all the channels data from the channels_config.

    Using:
    https://developers.google.com/resources/api-libraries/documentation/youtube/v3/python/latest/youtube_v3.channels.html

    Arguments:
        cur_chanels_config (DataFrame): A channels config dataframe see: ./channels_config.py
        parts_list: An optional list of parts that should be requested. Default is None.
            If None then DEFAULT_PARTS_TO_REQUEST is used.
            See:
            https://developers.google.com/youtube/v3/docs/channels/list#part
        client: YouTube client to override the default

    Returns:
        List of dictionaries that contain the channel list resource.
    """
    channels = utils.get_resource_client("channels", client)
    part_str = _get_part_str(parts_list)
    channel_ids_str = _get_channel_ids_str(cur_chanels_config)
    request = channels.list(part=part_str, id=channel_ids_str)
    result: List[Any] = []
    while request is not None:
        found_resource = request.execute()
        result = _process_found_channels(result, found_resource)
        request = channels.list_next(request, found_resource)
    return result


def _get_channel_ids_str(cur_chanels_config: pd.DataFrame):
    """Get the channel_ids_str."""
    return ",".join(cur_chanels_config["channel_id"].values)


def _get_part_str(
    parts_list: Optional[List[str]] = None,
) -> str:
    """Get the part string for the request."""
    if not parts_list:
        parts_list = DEFAULT_PARTS_TO_REQUEST
    return ",".join(parts_list)


def _process_found_channels(
    result: List[Any],
    found_channels: Dict[Any, Any],
) -> List[Any]:
    """Process the found channels appending to result."""
    return result + [found_channels]
