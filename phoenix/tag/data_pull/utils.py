"""Utils for pulling data."""
from typing import Any, List, Optional, Tuple

import datetime
import hashlib
import json
import logging

import pandas as pd
import tentaclio

from phoenix.common.artifacts import source_file_name_processing


JSONType = Any


logger = logging.getLogger(__name__)


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


def get_jsons(url_to_folder: str) -> List[Tuple[JSONType, datetime.datetime]]:
    """Read all JSON files and tuple with the file's timestamp."""
    json_objects: List[Tuple[Any, datetime.datetime]] = []
    for entry in tentaclio.listdir(url_to_folder):
        logger.info(f"Processing file: {entry}")
        if not is_valid_file_name(entry):
            logger.info(f"Skipping file with invalid filename: {entry}")
            continue
        file_timestamp = get_file_name_timestamp(entry)
        with tentaclio.open(entry) as file_io:
            json_objects.append((json.load(file_io), file_timestamp))
    return json_objects


def filter_df(
    df: pd.DataFrame, year_filter: Optional[int] = None, month_filter: Optional[int] = None
):
    """Filter dataframe by year and/or month."""
    if year_filter:
        df = df[df["year_filter"] == year_filter]
    if month_filter:
        df = df[df["month_filter"] == month_filter]
    return df
