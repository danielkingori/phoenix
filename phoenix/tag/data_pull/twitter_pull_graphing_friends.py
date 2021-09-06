"""Data pulling for Twitter friends graphing."""

import json

import pandas as pd
import tentaclio


def twitter_json(url_to_folder: str) -> dict:
    """Get last json file in the directory."""
    for entry in tentaclio.listdir(url_to_folder):
        pass
    # Get the last one in case it was run multiple times.
    # This assumes tentaclio reads in ascending timestamp.
    with tentaclio.open(entry) as file_io:
        friends = json.loads(file_io.read())
    return friends


def organize_users(friends: dict) -> list:
    """Isolate the friends list."""
    relations = []
    for user in friends.keys():
        for friend in friends[user]:
            relations.append(
                {
                    "user_1": user,
                    "user_2": friend,
                }
            )
    return relations


def get_friends_dataframe(friends: list) -> pd.DataFrame:
    """Return dataframe from list of dict."""
    return pd.DataFrame.from_dict(friends)


def get_friends(url: str) -> pd.DataFrame:
    """Run the data pull for twitter friends."""
    friends_raw = twitter_json(url)
    friends = organize_users(friends_raw)
    return get_friends_dataframe(friends)
