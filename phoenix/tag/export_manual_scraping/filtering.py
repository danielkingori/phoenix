"""Filtering."""
from typing import List, Optional

import pandas as pd


DEFAULT_ORDER_BY = "total_interactions"


def filter_posts(
    facebook_posts_df: pd.DataFrame,
    head: int,
    include_accounts: Optional[List[str]],
    has_topics: Optional[bool] = False,
    order_by: Optional[str] = DEFAULT_ORDER_BY,
) -> pd.DataFrame:
    """Filter the facebook posts for the export manual scraping."""
    df = facebook_posts_df.copy()
    if include_accounts:
        df = df[df["account_handle"].isin(include_accounts)]

    if has_topics:
        df = df.loc[df["has_topics"]]

    df = df.sort_values(by=order_by, ascending=False)

    return df.head(head)
