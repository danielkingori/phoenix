"""Filtering."""
from typing import List, Optional, Tuple

import pandas as pd


DEFAULT_ORDER_BY = "total_interactions"


def filter_posts(
    facebook_posts_df: pd.DataFrame,
    head: int,
    include_accounts: Optional[List[str]],
    has_topics: Optional[bool] = False,
    order_by: Optional[str] = DEFAULT_ORDER_BY,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filter the facebook posts for the export manual scraping.

    Args:
        facebook_posts_df: Dataframe to filter,
        head: number of posts to return,
        include_accounts: List of accounts to filter by,
        has_topics: if true filter by has_topics (relevant),
        order_by: Order by before the cut, default is total_interactions

    Return:
        A tuple with:
        (
            Facebook posts filtered,
            List of all matched accounts include those not in the cut
        )
    """
    df = facebook_posts_df.copy()
    if include_accounts:
        df = df[df["account_handle"].isin(include_accounts)]

    if has_topics:
        df = df.loc[df["has_topics"]]

    df = df.sort_values(by=order_by, ascending=False)

    found_accounts = df["account_handle"].unique()

    return df.head(head), found_accounts
