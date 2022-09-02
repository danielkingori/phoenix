"""Facebook graph module."""
from typing import Any, Dict, List, Optional, Tuple, Union

import dataclasses
import datetime
import logging
import os

import facebook
import requests

from phoenix.common.run_params import base, general


FACEBOOK_GRAPH_URL = "https://graph.facebook.com/"
FACEBOOK_GRAPH_AUTH_URL = f"{FACEBOOK_GRAPH_URL}oauth/access_token"
DEFAULT_POST_FIELDS = "id,message,created_time,from,shares,updated_time"


def get_graph(access_token: str):
    """Get the graph object."""
    # For version above 3 we need to have this merged:
    # https://github.com/mobolic/facebook-sdk/pull/503
    version = os.environ["FACEBOOK_API_VERSION"]
    logging.info(f"Version: {version}")
    if version:
        return facebook.GraphAPI(access_token=access_token, version=version)

    return facebook.GraphAPI(
        access_token=access_token,
    )


def get_app_access_token(grant_type="client_credentials"):
    """Get the app access token.

    Based on documentation:
    https://developers.facebook.com/docs/facebook-login/guides/access-tokens#apptokens
    """
    app_id = os.environ["FACEBOOK_APP_ID"]
    client_secret = os.environ["FACEBOOK_SECRET_TOKEN"]
    payload = {"client_id": app_id, "client_secret": client_secret, "grant_type": grant_type}

    r = requests.get(FACEBOOK_GRAPH_AUTH_URL, params=payload)
    r.raise_for_status()
    result = r.json()
    return result.get("access_token")


def get_client_access_token():
    """Get the client access token.

    Based on documentation:
    https://developers.facebook.com/docs/facebook-login/guides/access-tokens#clienttokens
    """
    app_id = os.environ["FACEBOOK_APP_ID"]
    client_token = os.environ["FACEBOOK_CLIENT_TOKEN"]
    if app_id and client_token:
        return f"{app_id}|{client_token}"
    m = "FACEBOOK_CLIENT_ID and FACEBOOK_CLIENT_ID must be set to create a client access_token."
    m += " Current values: FACEBOOK_CLIENT_ID: {app_id}, FACEBOOK_APP_ID: {client_token}"
    raise ValueError(m)


def get_feed_of_a_page(graph, page_id: str, fields=DEFAULT_POST_FIELDS, pagination_limit=None):
    """Get the feed of a page."""
    feed_result = []
    path = f"{page_id}/feed"
    status = 200
    nextPage = "placeholder"
    page_count = 0
    args = {"fields": fields}
    while status == 200 and nextPage:
        r = graph.request(path=path, args=args)
        found_feed, nextPage = _process_response_data(r)
        path = nextPage
        feed_result.extend(found_feed)
        page_count += 1
        if pagination_limit and page_count >= pagination_limit:
            break
    return feed_result


def _process_response_data(response_json):
    """Process the response to get the status, posts and nextPage."""
    feed = response_json.get("data", [])
    nextPage = response_json.get("paging", {}).get("cursors", {}).get("nextPage", None)
    return feed, nextPage


@dataclasses.dataclass
class FacebookFeedScrapeRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    source: str
    base: str


@dataclasses.dataclass
class FacebookFeedScrapeRunParams(base.RunParams):
    """Finalise accounts run params."""

    urls: FacebookFeedScrapeRunParamsURLs
    general: general.GeneralRunParams
    graph: Any
    page_id: str
    pagination_limit: Optional[int]


def create_feed_run_params(
    access_token: str,
    page_id: str,
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    pagination_limit: Optional[int],
):
    """Create the run params for the facebook_graph feed."""
    if not access_token or page_id:
        ValueError("Must have ACCESS_TOKEN and PAGE_ID set.")

    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    art_url_reg = general_run_params.art_url_reg

    url_config: Dict[str, Any] = {}
    urls = FacebookFeedScrapeRunParamsURLs(
        config=url_config,
        source=art_url_reg.get_url("source-facebook_feed", url_config),
        base=art_url_reg.get_url("base-facebook_feed", url_config),
    )
    graph = get_graph(access_token)
    return FacebookFeedScrapeRunParams(
        general=general_run_params,
        urls=urls,
        graph=graph,
        page_id=page_id,
        pagination_limit=pagination_limit,
    )
