"""YouTube videos data pull."""
from typing import Optional

import pandas as pd


def execute(
    url_to_folder: str, year_filter: Optional[int] = None, month_filter: Optional[int] = None
) -> pd.DataFrame:
    """Pull source json and create youtube_videos pre-tagging dataframe."""
    return pd.DataFrame({})
