"""Data pulling for Twitter graphing."""

import datetime
import json
import logging

import pandas as pd
import tentaclio
from dateutil.parser import parse

from phoenix.scrape import twitter_utilities
from phoenix.tag.data_pull import utils


def insert_file_timestamp(batch: list, file_timestamp: datetime.datetime) -> list:
    """Inserts the file timestamp into each row."""
    for tweet in batch:
        tweet["file_timestamp"] = file_timestamp
    return batch


def twitter_json(url_to_folder: str) -> list:
    """Get all the jsons and return a list with tweet data."""
    tweets = []
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        file_timestamp = utils.get_file_name_timestamp(entry)
        with tentaclio.open(entry) as file_io:
            batch = json.loads(file_io.read())
        batch = insert_file_timestamp(batch, file_timestamp)
        tweets.extend(batch)
    return tweets


def isolate_year(timestamp: str) -> int:
    """Isolate year from timestamp."""
    return parse(timestamp, fuzzy=True).year


def isolate_month(timestamp: str) -> int:
    """Isolate month from timestamp."""
    return parse(timestamp, fuzzy=True).month


def normalize_tweets_rt_graph(tweets: list) -> pd.DataFrame:
    """Normalize tweets for the retweet graph.

    Filter out tweets that are from any month not included in file timestamp.
    """
    # TODO: Filter out any tweet from the previous month.
    retweets_normalized = []
    for tweet in tweets:
        tweet_year = isolate_year(tweet["created_at"])
        tweet_month = isolate_month(tweet["created_at"])
        # Sometimes there is a Key Error on tweet["retweeted_status"] and
        # I can't figure out how to preemptively stop it anywhere else so
        # I am catching it here. This is maybe 5-10% of retweets.

        if tweet["file_timestamp"].month == tweet_month:
            try:
                retweets_normalized.append(
                    {
                        "id_str": tweet["id_str"],
                        "original_screen_name": tweet["retweeted_status"]["user"]["screen_name"],
                        "retweet_screen_name": tweet["user"]["screen_name"],
                        "file_timestamp": tweet["file_timestamp"],
                        "tweet_year": tweet_year,
                        "tweet_month": tweet_month,
                    }
                )
            except KeyError:
                # This is a retweeted tweet without "retweeted_status" in keys
                pass
    return retweets_normalized


def create_retweets_df(retweets):
    """Create a dataframe for retweets."""
    return pd.DataFrame.from_dict(retweets)


def filter_retweets(tweets: list) -> list:
    """Return only retweets from a list of tweets."""
    retweets = []
    for tweet in tweets:
        if twitter_utilities.is_tweet_a_retweet_dict(tweet):
            retweets.append(tweet)
    return retweets


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate tweets from dataframe, keeping the most recent versions."""
    df = df.sort_values("file_timestamp")
    df = df.groupby("id_str").last()
    df = df.reset_index()
    return df


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


def process_tweets_rt_graph(tweets: list, users: list) -> pd.DataFrame:
    """Collect the tweets data and organize it for graphing."""
    # tweets = twitter_json(url_to_folder)
    retweets = filter_retweets(tweets)
    normalized_retweets = normalize_tweets_rt_graph(retweets)
    retweets_df = create_retweets_df(normalized_retweets)
    retweets_df = remove_duplicates(retweets_df)
    retweets_df = calculate_weights_rt_graph(retweets_df)
    retweets_df = compare_users(retweets_df, users)
    return retweets_df
