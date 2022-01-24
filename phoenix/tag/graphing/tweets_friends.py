"""Processing and config for twitter_friends graph."""
from typing import Tuple

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry, processing_utilities


def process(
    twitter_friends: pd.DataFrame, final_accounts: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process twitter friends and twitter accounts into edges and nodes data."""
    # edges
    edges_df = twitter_friends.rename(columns={"followed_user_id": "followed_user_screen_name"})
    users_df = twitter_friends[["user_screen_name"]]
    users_2_df = twitter_friends[["followed_user_id"]].rename(
        columns={"followed_user_id": "user_screen_name"}
    )
    users_df = pd.concat([users_df, users_2_df]).drop_duplicates()

    users_df = users_df.merge(
        final_accounts, left_on="user_screen_name", right_on="object_user_name", how="left"
    )

    users_df = users_df[["user_screen_name", "account_label"]].fillna("")
    nodes_df = processing_utilities.reduce_concat_classes(
        users_df, ["user_screen_name"], "account_label"
    )

    nodes_df = nodes_df.sort_values(by="user_screen_name").reset_index(drop=True)

    return (edges_df, nodes_df)


plot_config = phoenix_graphistry.PlotConfig(
    edge_source_col="user_screen_name",
    edge_destination_col="followed_user_screen_name",
    nodes_col="user_screen_name",
    graph_name="twitter_friends",
    graph_description="""
        Graph showing friends between Twitter accounts.
        Nodes: Twitter accounts
        Edges: Exist where any twitter account follows an other account.
    """,
)
