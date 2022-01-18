"""Processing and config for tweets_retweets graph."""
from typing import Tuple

import pandas as pd


def process(
    final_tweets: pd.DataFrame, final_accounts: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process tweets and twitter accounts into retweets edges and nodes data."""
    # edges
    df = final_tweets[["user_screen_name", "retweeted_user_screen_name"]]
    df = df.groupby(["user_screen_name", "retweeted_user_screen_name"]).size().reset_index()
    edges_df = df.rename(
        columns={"user_screen_name": "tweeting_user_screen_name", 0: "times_retweeted"}
    )

    # nodes
    df = final_tweets[["user_screen_name", "retweeted_user_screen_name"]]
    nodes_df = pd.DataFrame(
        pd.concat([edges_df["retweeted_user_screen_name"], edges_df["tweeting_user_screen_name"]])
    )
    nodes_df = nodes_df.rename(columns={0: "user_screen_name"})
    nodes_df = nodes_df.drop_duplicates(ignore_index=True)
    account_labels_df = final_accounts.groupby("object_user_name")["account_label"].apply(list)
    account_labels_df = account_labels_df.reset_index()
    account_labels_df["account_label"] = account_labels_df["account_label"].apply(
        lambda l: ", ".join(sorted(l))
    )
    nodes_df = nodes_df.merge(
        account_labels_df, how="outer", left_on="user_screen_name", right_on="object_user_name"
    )
    nodes_df["user_screen_name"] = nodes_df["user_screen_name"].fillna(
        nodes_df["object_user_name"]
    )
    nodes_df = (
        nodes_df[["user_screen_name", "account_label"]]
        .sort_values(by="user_screen_name")
        .reset_index(drop=True)
    )

    return (edges_df, nodes_df)
