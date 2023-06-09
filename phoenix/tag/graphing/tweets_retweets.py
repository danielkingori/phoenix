"""Processing and config for tweets_retweets graph."""
from typing import Tuple

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry, processing_utilities


def process(
    final_tweets_classes: pd.DataFrame, final_accounts: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process tweets and twitter accounts into retweets edges and nodes data."""
    # edges
    edges_classes_df = final_tweets_classes[
        [
            "user_screen_name",
            "retweeted_user_screen_name",
            "class",
        ]
    ]
    edges_classes_df = processing_utilities.reduce_concat_classes(
        edges_classes_df, ["user_screen_name", "retweeted_user_screen_name"], "class"
    )
    edges_classes_df = edges_classes_df.rename(
        columns={"user_screen_name": "tweeting_user_screen_name"}
    )

    df = final_tweets_classes[
        ["id", "user_screen_name", "retweeted_user_screen_name"]
    ].drop_duplicates(subset="id", keep="first")
    df = df[["user_screen_name", "retweeted_user_screen_name"]]
    df = df.groupby(["user_screen_name", "retweeted_user_screen_name"]).size().reset_index()
    df = df.rename(columns={"user_screen_name": "tweeting_user_screen_name", 0: "times_retweeted"})
    edges_df = df.merge(
        edges_classes_df,
        how="inner",
        on=["tweeting_user_screen_name", "retweeted_user_screen_name"],
        validate="one_to_one",
    )

    # nodes
    nodes_df = pd.DataFrame(
        pd.concat([edges_df["retweeted_user_screen_name"], edges_df["tweeting_user_screen_name"]])
    )
    nodes_df = nodes_df.rename(columns={0: "user_screen_name"})
    nodes_df = nodes_df.drop_duplicates(ignore_index=True)
    account_labels_df = final_accounts[["object_user_name", "account_label"]]
    account_labels_df = processing_utilities.reduce_concat_classes(
        account_labels_df, ["object_user_name"], "account_label"
    )

    nodes_df = nodes_df.merge(
        account_labels_df, how="outer", left_on="user_screen_name", right_on="object_user_name"
    )
    nodes_df["user_screen_name"] = nodes_df["user_screen_name"].fillna(
        nodes_df["object_user_name"]
    )
    nodes_df = nodes_df[["user_screen_name", "account_label"]]
    nodes_df = nodes_df.sort_values(by="user_screen_name").reset_index(drop=True)

    return (edges_df, nodes_df)


plot_config = phoenix_graphistry.PlotConfig(
    edge_source_col="retweeted_user_screen_name",
    edge_destination_col="tweeting_user_screen_name",
    nodes_col="user_screen_name",
    graph_name="tweets_retweets",
    graph_description="""
        Graph showing retweets between Twitter accounts.
        Nodes: Twitter accounts
        Edges: Exist where any retweeting of source by destination account has occurred. Edge
            weight is number of times retweeting has occurred.
    """,
    edge_weight_col="times_retweeted",
)
