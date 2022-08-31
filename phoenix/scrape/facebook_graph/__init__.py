"""Facebook graph module."""
import logging
import os

import facebook
import requests


FACEBOOK_GRAPH_URL = "https://graph.facebook.com/"
FACEBOOK_GRAPH_AUTH_URL = f"{FACEBOOK_GRAPH_URL}oauth/access_token"


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
