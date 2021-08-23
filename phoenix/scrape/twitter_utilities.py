"""Utilities for twitter queries."""
import datetime
import re

import tentaclio
import tweepy


def find_hashtags_in_tweet_text(full_text) -> list:
    """Find hashtags in the tweet text."""
    hashtag_re = r"(#[A-Za-z0-9]*)"
    hashtags_found = re.findall(hashtag_re, full_text)
    return [h.strip("#").lower() for h in hashtags_found]


def is_tweet_a_retweet(tweet: tweepy.Status) -> bool:
    """Check if tweet is a retweet from tweepy status."""
    retweet_re = r"(RT @)"
    if re.match(retweet_re, tweet.full_text) or tweet.retweeted:
        return True
    return False


def is_tweet_a_retweet_dict(tweet: dict) -> bool:
    """Check if tweet is a retweet."""
    retweet_re = r"(RT @)"
    if re.match(retweet_re, tweet["full_text"]) or "retweeted_stats" in tweet.keys():
        return True
    return False


def is_recent_tweet(since_days, status: tweepy.Status) -> bool:
    """Check if the tweet is recent within given dates."""
    today = datetime.datetime.now()
    compare_time = today - datetime.timedelta(since_days)
    status_time = status.created_at
    return status_time >= compare_time


def load_queries_from_csv(filepath) -> list:
    """Load list of query terms from a local csv."""
    queries = []
    with tentaclio.open(filepath) as f:
        queries = [row.strip() for row in f]
    return queries
