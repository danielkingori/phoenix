"""Data pulling for Twitter friends graphing."""

import json

import pandas as pd
import tentaclio


def twitter_json(url_to_folder: str) -> list:
    """Get last json file in the directory."""
    for entry in tentaclio.listdir(url_to_folder):
        pass
    # Get the last one in case it was run multiple times.
    # This assumes tentaclio reads in ascending timestamp.
    with tentaclio.open(entry) as file_io:
        friends = json.loads(file_io.read())
    return friends


def isolate_users(friends: list) -> list:
    """Isolate the friends list."""
    relations = []
    for friend in friends:
        relations.append(
            {
                "user_1": friend["query_user"],
                "user_2": friend["screen_name"],
            }
        )
    return relations


def get_friends_dataframe(friends: list) -> pd.DataFrame:
    """Return dataframe from list of dict."""
    return pd.DataFrame.from_dict(friends)


def get_friends(url: str) -> pd.DataFrame:
    """Run the data pull for twitter friends."""
    friends_raw = twitter_json(url)
    friends = isolate_users(friends_raw)
    return get_friends_dataframe(friends)
