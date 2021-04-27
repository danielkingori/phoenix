import os
import tweepy

ENV_C_KEY = 'TWITTER_CONSUMER_KEY'
ENV_C_SECRET = 'TWITTER_CONSUMER_SECRET'
ENV_A_TOKEN = 'TWITTER_APPLICATION_TOKEN'
ENV_A_SECRET = 'TWITTER_APPLICATION_SECRET'


QUERY = '' # Default query for tweet_search(api, q)

NUM_ITEMS = 5 # Instructs the cursor how many items to retrieve. \
              # Different results for different API calls that have different rate limits


def get_key(key_name):
    key = os.getenv(key_name)
    if not key:
        raise ValueError(f"No key found for env {key_name}")
    return key


def connect_twitter_api(token: dict = None):
    # Connect to twitter API v1
    # Set the access token if someone allows twitter app connection
    auth = tweepy.OAuthHandler(get_key(ENV_C_KEY),
                               get_key(ENV_C_SECRET))
    auth.set_access_token(
        token['oauth_token'       ] if token else get_key(ENV_A_TOKEN),
        token['oauth_token_secret'] if token else get_key(ENV_A_SECRET))

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def tweet_search(api, q: str = QUERY, num_items: int = NUM_ITEMS):
    """Twitter keyword search."""
    for status in tweepy.Cursor(api.search, q=q).items(num_items):
        yield status

def get_user_timeline(api, id = None, count = 200, num_items: int = NUM_ITEMS):
    """Twitter get statuses from timeline of given id or username."""
    for status in tweepy.Cursor(api.user_timeline, count=count, id=id).items(num_items):
        yield status
