import datetime
import re

import tweepy


def find_hashtags_in_tweet_text(full_text):
    hashtag_re = r"(#[A-Za-z0-9]*)"
    hashtags_found = re.findall(hashtag_re, full_text)
    return [h.strip("#").lower() for h in hashtags_found]


def is_tweet_a_retweet(tweet: tweepy.Status):
    retweet_re = r"(RT @)"
    if re.match(retweet_re, tweet.full_text) or tweet.retweeted:
        return True


def is_recent_tweet(since_days, status: tweepy.Status):
    today = datetime.datetime.now()
    compare_time = today - datetime.timedelta(since_days)
    status_time = status.created_at
    return status_time >= compare_time
