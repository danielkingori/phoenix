"""Scrape package for twitter friends."""
import logging

import tweepy

from phoenix.scrape.twitter_queries import _get_user_friends_tweet_cursor, connect_twitter_api


def get_friends(query, num_items, api):
    """Iterate through queries and call the api cursor function."""
    try:
        for friends in _get_user_friends_tweet_cursor(api, query, num_items):
            yield friends
    except tweepy.error.TweepError as e:
        if e.response.status_code == 401:
            logging.info(
                "401 Unauthorized: either bad api tokens or not \
                authorized to access the query due to a locked account"
            )


def get_friends_dict(queries: list, num_items, api):
    """Organize friends list as a dictionary."""
    friends_dict: dict = {}
    for query in queries:
        friends_dict[query] = []
        friends_dict[query].extend(get_friends(query, num_items, api))
    return friends_dict


def get_friends_json(queries: list, num_items=0) -> list:
    """Manage the friend collection process from Twitter."""
    api = connect_twitter_api()
    friends = get_friends_dict(queries, num_items, api)
    return friends
