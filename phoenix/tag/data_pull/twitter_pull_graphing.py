"""Data pulling for Twitter graphing."""

import json
import logging

import pandas as pd
import tentaclio

from phoenix.scrape import twitter_utilities


def twitter_json(url_to_folder: str) -> list:
    """Get all the jsons and return a list with tweet data."""
    tweets = []
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        # TODO: file_timestamp = utils.get_file_name_timestamp(entry)
        with tentaclio.open(entry) as file_io:
            tweets.extend(json.loads(file_io.read()))
            # TODO: tweets[-1]['file_timestamp'] = file_timestamp
    return tweets


def normalize_tweets_rt_graph(tweets: list) -> pd.DataFrame:
    """Normalize tweets for the retweet graph."""
    retweets = filter_retweets(tweets)
    # TODO: Filter out any tweet from the previous month.
    retweets_normalized = []
    for tweet in retweets:
        retweets_normalized.append(
            {
                "original_screen_name": tweet["retweeted_status"]["user"]["screen_name"],
                "retweet_screen_name": tweet["user"]["screen_name"],
            }
        )
    retweets_df = pd.DataFrame.from_dict(retweets_normalized)
    return retweets_df


def filter_retweets(tweets: list) -> list:
    """Return only retweets from a list of tweets."""
    retweets = []
    for tweet in tweets:
        if twitter_utilities.is_tweet_a_retweet_dict(tweet):
            retweets.append(tweet)
    return retweets


def calculate_weights_rt_graph(data: pd.DataFrame) -> pd.DataFrame:
    """Gets weight of each edge based on duplicates across users in collection."""
    return (
        data.groupby(["original_screen_name", "retweet_screen_name"])
        .size()
        .to_frame("count")
        .reset_index()
    )


def compare_users(df: pd.DataFrame, user_list: list) -> pd.DataFrame:
    """Check and mark with boolean if screen_names are in the user query list."""
    df["original_listed"] = df["original_screen_name"].isin(user_list)
    df["retweet_listed"] = df["retweet_screen_name"].isin(user_list)
    return df


def collect_tweets_rt_graph(url_to_folder: str, users: list) -> pd.DataFrame:
    """Collect the tweets data and organize it for graphing."""
    tweets = twitter_json(url_to_folder)
    retweets_df = normalize_tweets_rt_graph(tweets)
    retweets_df = calculate_weights_rt_graph(retweets_df)
    retweets_df = compare_users(retweets_df, users)
    return retweets_df
