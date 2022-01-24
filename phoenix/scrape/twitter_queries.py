"""Twitter query connections through the Twitter API."""
import logging
import os

import tweepy

from phoenix.scrape import twitter_utilities


# User authentication OAuth 1
ENV_CONSUMER_KEY = "TWITTER_CONSUMER_KEY"
ENV_CONSUMER_SECRET = "TWITTER_CONSUMER_SECRET"
ENV_OAUTH_ACCESS_TOKEN = "TWITTER_OAUTH_ACCESS_TOKEN"
ENV_OAUTH_ACCESS_SECRET = "TWITTER_OAUTH_ACCESS_SECRET"

# Application authentication OAuth 2
ENV_APPLICATION_KEY = "TWITTER_APPLICATION_KEY"
ENV_APPLICATION_SECRET = "TWITTER_APPLICATION_SECRET"


QUERY = ""  # Default query for tweet_search(api, q)

NUM_ITEMS = None  # Instructs the cursor how many items to retrieve. \
# Different results for different API calls that have different rate limits


def get_auth_handler() -> tweepy.auth.AuthHandler:
    """Get the auth handler based on environment variables."""
    access_token_key = os.getenv(ENV_OAUTH_ACCESS_TOKEN)
    access_token_secret = os.getenv(ENV_OAUTH_ACCESS_SECRET)
    consumer_key = os.getenv(ENV_CONSUMER_KEY)
    consumer_secret = os.getenv(ENV_CONSUMER_SECRET)
    if consumer_key and consumer_secret and access_token_key and access_token_secret:
        logging.info("Using OAuthHandler for tweepy authentication.")
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(
            access_token_key,
            access_token_secret,
        )
        return auth

    applicaton_key = os.getenv(ENV_APPLICATION_KEY)
    applicaton_secret = os.getenv(ENV_APPLICATION_SECRET)
    if applicaton_secret and applicaton_key:
        logging.info("Using App authentication for tweepy authentication.")
        return tweepy.AppAuthHandler(applicaton_key, applicaton_secret)

    raise RuntimeError("Authentication for Twitter is not correctly configured.")


def connect_twitter_api() -> tweepy.API:
    """Connect to twitter API v1."""
    auth = get_auth_handler()
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return api


def _tweet_search_cursor(api, query, num_items) -> tweepy.Cursor:
    """Manages the cursor for Twitter api.search endpoint."""
    return tweepy.Cursor(
        api.search,
        q=query,
        count=100,
        results="recent",
        tweet_mode="extended",
    ).items(num_items)


def _get_user_tweet_cursor(api, id, num_items) -> tweepy.Status:
    """Manages the cursor for Twitter api.user_timeline endpoint."""
    return tweepy.Cursor(
        api.user_timeline,
        id=id,
        count=200,
        tweet_mode="extended",
    ).items(num_items)


def _get_user_friends_tweet_cursor(api, screen_name, num_items) -> tweepy.Status:
    """Manages cursor for Twitter api.friends endpoint."""
    return tweepy.Cursor(
        api.friends_ids, screen_name=screen_name, count=5000, skip_status=True
    ).items(num_items)


def get_tweets_since_days(query_type, query, since_days, num_items, api=None) -> tweepy.Status:
    """Decides if query is for users or keywords, and checks \
    if the returns are within the since_days timeframe."""
    # Check API
    if not api:
        api = connect_twitter_api()
    # Check query type
    logging.info(f"Query_type: {query_type} | Query: {query}")
    if query_type == "user":
        cursor_function = _get_user_tweet_cursor(api, query, num_items)
    elif query_type == "keyword":
        cursor_function = _tweet_search_cursor(api, query, num_items)
    else:
        logging.info("Bad query.")
        raise ValueError(f"No query for query type: {query_type}")
    # Run query
    try:
        for status in cursor_function:
            if twitter_utilities.is_recent_tweet(since_days, status):
                yield status
            else:
                break
    except tweepy.error.TweepError as e:
        if e.response.status_code == 401:
            logging.info(
                "401 Unauthorized: either bad api tokens or not \
                authorized to access the query due to a locked account"
            )


def get_tweets(query_type, queries, num_items, since_days, api) -> list:
    """Collects returned tweets."""
    tweets = []
    for query in queries:
        returned_tweets = get_tweets_since_days(query_type, query, since_days, num_items, api)
        tweets.extend(returned_tweets)
    return tweets


def get_tweets_json(query_type: str, queries: list, num_items=0, since_days=1) -> list:
    """Extracts json from returned tweets and puts them into a DataFrame."""
    api = connect_twitter_api()
    tweets = get_tweets(query_type, queries, num_items, since_days, api)
    return extract_tweet_json(tweets)
    # note: currently if no tweets are found, this crashes with an IndexError
    # previously this was ->def get_tweets_dataframe():
    #                       return pd.DataFrame(tweets_json, columns=tweets_json[0].keys())


def extract_tweet_json(tweets):
    """Extracts json element of tweet object."""
    if not tweets:
        tweets_json = []
    else:
        tweets_json = [tweet._json for tweet in tweets]
    return tweets_json


def enrich_with_query_user(friend, query):
    """Add the query user to return data."""
    friend_json = friend._json
    friend_json["query_user"] = query
    return friend_json
