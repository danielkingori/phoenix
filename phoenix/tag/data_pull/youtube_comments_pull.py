"""YouTube comments data pull (i.e. processing of raw scraped data.

Specifically this processing raw json data of kind `youtube#commentThreadListResponse`.
"""
from typing import Optional

import pandas as pd


def from_json(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Process source json into youtube_comments pre-tagging dataframe."""
    pass


def for_tagging(given_df: pd.DataFrame):
    """Get YouTube comments for tagging.

    Return:
    dataframe  : pandas.DataFrame
    Index:
        object_id: String, dtype: string
    Columns:
        object_id: String, dtype: string
        text: String, dtype: string
        object_type: "facebook_post", dtype: String
        created_at: datetime
        object_url: String, dtype: string
        object_user_url: String, dtype: string
        object_user_name: String, dtype: string
    """
    pass
