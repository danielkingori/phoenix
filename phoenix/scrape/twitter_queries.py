import os

import pandas as pd
import tweepy

from phoenix.scrape import twitter_utilities


ENV_C_KEY = "TWITTER_CONSUMER_KEY"
ENV_C_SECRET = "TWITTER_CONSUMER_SECRET"
ENV_A_TOKEN = "TWITTER_APPLICATION_TOKEN"
ENV_A_SECRET = "TWITTER_APPLICATION_SECRET"


QUERY = ""  # Default query for tweet_search(api, q)

NUM_ITEMS = None  # Instructs the cursor how many items to retrieve. \
# Different results for different API calls that have different rate limits


def get_key(key_name):
    key = os.getenv(key_name)
    if not key:
        raise ValueError(f"No key found for env {key_name}")
    return key


def connect_twitter_api(token: dict = None) -> tweepy.API:
    # Connect to twitter API v1
    # Set the access token if someone allows twitter app connection
    auth = tweepy.OAuthHandler(get_key(ENV_C_KEY), get_key(ENV_C_SECRET))
    auth.set_access_token(
        token["oauth_token"] if token else get_key(ENV_A_TOKEN),
        token["oauth_token_secret"] if token else get_key(ENV_A_SECRET),
    )

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def _tweet_search_cursor(api, query, num_items):
    """Manages the cursor for Twitter api.search endpoint."""
    return tweepy.Cursor(
        api.search,
        q=query,
        count=100,
        results="recent",
        extended=True,
    ).items(num_items)


def _get_user_tweet_cursor(api, id, num_items) -> tweepy.Status:
    """Manages the cursor for Twitter api.user_timeline endpoint."""
    return tweepy.Cursor(
        api.user_timeline,
        id=id,
        count=200,
    ).items(num_items)


def get_tweets_since_days(query_type, query, since_days, num_items, api=None) -> tweepy.Status:
    """Decides if query is for users or keywords, and checks if the returns are within the since_days timeframe."""
    # Check API
    print("running query")
    if not api:
        api = connect_twitter_api()
    # Check query type
    print(f"query_type: {query_type}")
    if query_type == "users":
        cursor_function = _get_user_tweet_cursor(api, query, num_items)
    elif query_type == "keywords":
        cursor_function = _tweet_search_cursor(api, query, num_items)
    else:
        print("BAD QUERY")
        raise ValueError(f"No query for query type: {query_type}")
    # Run query
    for status in cursor_function:
        if twitter_utilities.is_recent_tweet(since_days, status):
            yield status
        else:
            break


def get_tweets(query_type, queries, num_items, since_days, api):
    """Collects returned tweets."""
    tweets = []
    for query in queries:
        returned_tweets = get_tweets_since_days(
            query_type, query, since_days, num_items, api
        )
        tweets.extend(returned_tweets)
    return tweets


def get_tweets_dataframe(query_type: str, queries: list, num_items=0, since_days=1):
    """Extracts json from returned tweets and puts them into a DataFrame"""
    api = connect_twitter_api()
    tweets = get_tweets(query_type, queries, num_items, since_days, api)
    tweets_json = [tweet._json for tweet in tweets]
    # note: currently if no tweets are found, this crashes with an IndexError
    return pd.DataFrame(tweets_json, columns=tweets_json[0].keys())
