"""Crowdtangle.

Interface with the api:
https://github.com/CrowdTangle/API/wiki
"""
from typing import Any, Dict, List

import copy
import datetime
import logging
import os
import time

import requests

from phoenix.common import constants


POSTS_BASE_URL = "https://api.crowdtangle.com/posts"

TOKEN_ENV_NAME = "CROWDTANGLE_API_TOKEN"

DEFAULT_RATE_LIMIT_CALLS = 6
DEFAULT_RATE_LIMIT_SECONDS = 60
RATE_LIMIT_CALLS_ENV_NAME = "CT_RATE_LIMIT_CALLS"
RATE_LIMIT_MINUTES_ENV_NAME = "CT_RATE_LIMIT_SECONDS"


def get_rate_limits():
    """Get the rate limit for the Crowdtangle API."""
    # Set defaults
    rate_limit_calls = DEFAULT_RATE_LIMIT_CALLS
    rate_limit_seconds = DEFAULT_RATE_LIMIT_SECONDS
    # Load from env
    rate_limit_calls_env = os.getenv(RATE_LIMIT_CALLS_ENV_NAME)
    rate_limit_seconds_env = os.getenv(RATE_LIMIT_MINUTES_ENV_NAME)
    # If loaded, convert str to int
    if rate_limit_calls_env:
        rate_limit_calls = int(rate_limit_calls_env)
    if rate_limit_seconds_env:
        rate_limit_seconds = int(rate_limit_seconds_env)
    return rate_limit_calls, rate_limit_seconds


def get_auth_token():
    """Get the authorisation token."""
    token = os.getenv(TOKEN_ENV_NAME)
    if not token:
        raise RuntimeError(f"No token found for env {TOKEN_ENV_NAME}")
    return token


def get_post(url: str, payload: Dict[str, Any]):
    """Get a post from crowdtangle."""
    token = get_auth_token()
    safe_url = url.replace(token, "****")
    safe_payload = copy.deepcopy(payload)
    if "token" in safe_payload:
        safe_payload["token"] = "****"
    logging.info(f"Making request {safe_url}, payload {safe_payload}")
    r = requests.get(url, params=payload, headers={"x-api-token": get_auth_token()})
    r.raise_for_status()
    return r.json()


def get_all_posts(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    list_ids: List[str],
):
    """Get all the posts for search params."""
    posts = []
    payload = {
        "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "listIds": list_ids,
        "sortBy": constants.FACEBOOK_POST_SORT_BY,
        "count": 100,
    }

    url = POSTS_BASE_URL
    status = 200
    nextPage = "placeholder"
    # get rate limits
    rate_limit_calls, rate_limit_seconds = get_rate_limits()
    logging.info(f"Rate limit: {rate_limit_calls} requests per {rate_limit_seconds} seconds.")
    # Doing the pagination based on
    # https://github.com/CrowdTangle/API/wiki/Pagination
    while status == 200 and nextPage:
        r = get_post(url, payload)
        status, found_posts, nextPage = _process_response_data(r)
        url = nextPage
        payload = {}
        posts.extend(found_posts)
        # Slow down for rate limit
        time.sleep(rate_limit_seconds / rate_limit_calls + 0.5)
    return posts


def _process_response_data(r):
    """Process the response to get the status, posts and nextPage."""
    status = r.get("status", None)
    result = r.get("result", {})
    posts = result.get("posts", [])
    nextPage = result.get("pagination", {}).get("nextPage", None)
    return status, posts, nextPage
