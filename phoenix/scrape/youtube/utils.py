"""Utils for youtube."""
from typing import List, Optional

import os

from googleapiclient import discovery
from googleapiclient.http import HttpMock


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
API_KEY_ENV_NAME = "YOUTUBE_API_KEY"


def get_client(http_mock: Optional[HttpMock] = None) -> discovery.Resource:
    """Get the client.

    Arguments:
        http_mock (HttpMock): used to mock the client.

    Returns:
        youtube resource
    """
    api_key = get_api_key_from_env()
    return discovery.build(
        YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key, http=http_mock
    )


def get_api_key_from_env():
    """Get the api key from the env."""
    key = os.getenv(API_KEY_ENV_NAME)
    if not key:
        raise RuntimeError(f"No key found for env {API_KEY_ENV_NAME}")
    return key


def get_resource_client(
    resource_name: str,
    client: Optional[discovery.Resource] = None,
):
    """Get the Resource Client form the client.

    Arguments:
        resource_name (str): name of resource to get from the client.
        client: youtube client. Optional

    Returns:
        Resource
    """
    if not client:
        client = get_client()
    return getattr(client, resource_name)()


def get_part_str(
    default_pasts_list: List[str],
    parts_list: Optional[List[str]] = None,
) -> str:
    """Get the part string for the request."""
    if not parts_list:
        parts_list = default_pasts_list
    return ",".join(parts_list)
