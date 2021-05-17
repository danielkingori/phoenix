"""Crowdtangle.

Interface with the api:
https://github.com/CrowdTangle/API/wiki
"""
from typing import Any, Dict, List

import datetime
import logging
import os
import time

import requests


POSTS_BASE_URL = "https://api.crowdtangle.com/posts"

TOKEN_ENV_NAME = "CROWDTANGLE_API_TOKEN"

# Rate Limit
RATE_LIMIT_CALLS = 6
RATE_LIMIT_SECONDS = 1 * 60  # 1 minute intervals


def get_auth_token():
    """Get the authorisation token."""
    token = os.getenv(TOKEN_ENV_NAME)
    if not token:
        raise RuntimeError(f"No token found for env {TOKEN_ENV_NAME}")
    return token


def get_post(url: str, payload: Dict[str, Any]):
    """Get a post from crowdtangle."""
    logging.info(f"Making request {url}, payload {payload}")
    r = requests.get(url, params=payload, headers={"x-api-token": get_auth_token()})
    r.raise_for_status()
    return r.json()


def get_all_posts(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    list_ids: List[str],
    sort_by="total_interactions",
):
    """Get all the posts for search params."""
    posts = []
    payload = {
        "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "listIds": list_ids,
        "sortBy": sort_by,
        "count": 100,
    }

    url = POSTS_BASE_URL
    status = 200
    nextPage = "placeholder"

    # Doing the pagination based on
    # https://github.com/CrowdTangle/API/wiki/Pagination
    while status == 200 and nextPage:
        r = get_post(url, payload)
        status, found_posts, nextPage = _process_response_data(r)
        url = nextPage
        payload = {}
        posts.extend(found_posts)
        # Slow down for rate limit
        time.sleep(RATE_LIMIT_SECONDS / RATE_LIMIT_CALLS + 0.5)
        # Facebook sends back a 429 response when the rate limit is met,
        # but I couldn't figure out how to quickly except that response
        # {"status":429,
        #  "code":32,
        #  "message":"Rate limit exceeded.
        #             Allowed limit: 6 requests per minute."}
    return posts


def _process_response_data(r):
    """Process the response to get the status, posts and nextPage."""
    status = r.get("status", None)
    result = r.get("result", {})
    posts = result.get("posts", [])
    nextPage = result.get("pagination", {}).get("nextPage", None)
    return status, posts, nextPage
