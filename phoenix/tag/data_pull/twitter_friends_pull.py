"""Pull Twitter friends data and organize it for Graphing."""

import json

import pandas as pd
import tentaclio


def twitter_json(url_to_file: str) -> dict:
    """Fetch data."""
    with tentaclio.open(url_to_file) as file_io:
        friends = json.loads(file_io.read())
    return friends


def organize_users(friends: dict) -> list:
    """Isolate the friends list."""
    relations = []
    for user in friends.keys():
        for friend in friends[user]:
            relations.append(
                {
                    "user_screen_name": user,
                    "followed_user_id": friend,
                }
            )
    return relations


def get_friends_dataframe(friends: list) -> pd.DataFrame:
    """Return dataframe from list of dict."""
    return pd.DataFrame.from_dict(friends)


def get_friends(data: dict) -> pd.DataFrame:
    """Run the data pull for twitter friends."""
    friends = organize_users(data)
    return get_friends_dataframe(friends)
