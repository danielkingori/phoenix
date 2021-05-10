import os
import tweepy
from phoenix.twitter import twitter_utilities

ENV_C_KEY = 'TWITTER_CONSUMER_KEY'
ENV_C_SECRET = 'TWITTER_CONSUMER_SECRET'
ENV_A_TOKEN = 'TWITTER_APPLICATION_TOKEN'
ENV_A_SECRET = 'TWITTER_APPLICATION_SECRET'


QUERY = '' # Default query for tweet_search(api, q)

NUM_ITEMS = None # Instructs the cursor how many items to retrieve. \
              # Different results for different API calls that have different rate limits


def get_key(key_name):
    key = os.getenv(key_name)
    if not key:
        raise ValueError(f"No key found for env {key_name}")
    return key


def connect_twitter_api(token: dict = None) -> tweepy.API:
    # Connect to twitter API v1
    # Set the access token if someone allows twitter app connection
    auth = tweepy.OAuthHandler(get_key(ENV_C_KEY),
                               get_key(ENV_C_SECRET))
    auth.set_access_token(
        token['oauth_token'       ] if token else get_key(ENV_A_TOKEN),
        token['oauth_token_secret'] if token else get_key(ENV_A_SECRET))

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def tweet_search(query: str = QUERY,
                 num_items = NUM_ITEMS) -> tweepy.Status:
    """Twitter keyword search."""
    api = connect_twitter_api()
    for status in tweepy.Cursor(api.search,
                                q=query,
                                count=100,
                                results='recent',
                                extended=True,).items(num_items):
        yield status


def _get_tweet_cursor(api, id=None, num_items=0, since_days=1) -> tweepy.Status:
    return tweepy.Cursor(
        api.user_timeline,
        id=id,
        count=200,
    ).items(num_items)


def get_user_timeline(api=None, id=None, num_items=0, since_days=1) -> tweepy.Status:
    """Twitter get statuses from timeline of given id or username."""
    if not api:
        api = connect_twitter_api()
    for status in _get_tweet_cursor(api, id, num_items, since_days):
        if twitter_utilities.is_recent_tweet(since_days, status):
            yield status
        else:
            break


def _get_tweets_for_ids(api, ids_list, num_items, since_days):
    tweets = []
    for twitter_id in ids_list:
        user_tweets = get_user_timeline(
            api, id=twitter_id, since_days=since_days, num_items=num_items
        )
        tweets.extend(user_tweets)

    return tweets


def get_user_tweets_dataframe(ids_list, num_items, since_days):
    api = connect_twitter_api()
    tweets_for_ids = _get_tweets_for_ids(api, ids_list, num_items, since_days)
    tweets_json = [tweet._json for tweet in tweets_for_ids]
    return pd.DataFrame(tweets_json, columns=tweets_json[0].keys())
