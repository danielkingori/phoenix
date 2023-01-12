"""Filtering."""
from typing import List, Optional, Tuple

import datetime

import pandas as pd


DEFAULT_ORDER_BY = "total_interactions"


def filter_posts(
    facebook_posts_df: pd.DataFrame,
    head: int,
    include_accounts: Optional[List[str]],
    has_topics: Optional[bool] = False,
    order_by: Optional[str] = DEFAULT_ORDER_BY,
    after_timestamp: Optional[datetime.datetime] = None,
    before_timestamp: Optional[datetime.datetime] = None,
    exclude_accounts: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filter the facebook posts for the export manual scraping.

    Args:
        facebook_posts_df: Dataframe to filter,
        head: number of posts to return,
        include_accounts: List of accounts to filter by,
        has_topics: if true filter by has_topics (relevant),
        order_by: Order by before the cut, default is total_interactions,
        after_timestamp: Posts created after a date time,
        before_timestamp: Posts created before a date time,
        exclude_accounts: List of accounts to exclude,

    Return:
        A tuple with:
        (
            Facebook posts filtered,
            List of all matched accounts include those not in the cut
        )
    """
    df = facebook_posts_df.copy()
    if include_accounts:
        df = df[df["account_platform_id"].isin(include_accounts)]

    if exclude_accounts:
        df = df[~df["account_platform_id"].isin(exclude_accounts)]

    if has_topics:
        df = df.loc[df["has_topics"]]

    if after_timestamp:
        df = df[df["timestamp_filter"] > after_timestamp]

    if before_timestamp:
        df = df[df["timestamp_filter"] < before_timestamp]

    df = df.sort_values(by=order_by, ascending=False)

    found_accounts = (
        df[["account_platform_id", "account_handle", "account_name"]]
        .drop_duplicates()
        .sort_values(by="account_platform_id", ascending=False)
        .reset_index(drop=True)
    )

    return df.head(head), found_accounts
