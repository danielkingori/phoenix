"""Utils for pulling data."""
import datetime
import hashlib

import pandas as pd

from phoenix.common.artifacts import source_file_name_processing


def to_type(column_name: str, astype, df: pd.DataFrame):
    """Name column string."""
    df[column_name] = df[column_name].astype(astype)
    return df


def hash_message(message: str):
    """Get the hash of a message."""
    return hashlib.md5(bytes(message, "utf-8")).hexdigest()[:16]


def get_file_name_timestamp(url: str) -> datetime.datetime:
    """From the file url get the timestamp.

    Raises:
        If there is no timestamp found.

    Returns
        datetime.datetime with aware UTC localization
    """
    source_file_name = source_file_name_processing.get_source_file_name(url)
    if not source_file_name:
        raise RuntimeError(
            (
                f"Unable to process file name {url}."
                " You may need to add a timestamp to the file name."
                " Check phoenix/common/run_datetime.py for format."
            )
        )
    return source_file_name.run_dt.dt


def is_valid_file_name(url: str) -> bool:
    """Valid file name check."""
    return url.endswith(".json")


def add_filter_cols(df: pd.DataFrame, created_at_col: pd.Series) -> pd.DataFrame:
    """Add the filter columns based on the created at col.

    Adds columns:
        - "timestamp_filter"
        - "date_filter"
        - "year_filter"
        - "month_filter"
        - "day_filter"

    Returns:
        New dataframe with the filter columns.
    """
    df = df.copy()
    df["timestamp_filter"] = created_at_col
    df["date_filter"] = created_at_col.dt.date
    df["year_filter"] = created_at_col.dt.year
    df["month_filter"] = created_at_col.dt.month
    df["day_filter"] = created_at_col.dt.day
    return df
