"""Utils for pulling data."""
import datetime
import hashlib
import os
import re

import pandas as pd


def to_type(column_name: str, astype, df: pd.DataFrame):
    """Name column string."""
    df[column_name] = df[column_name].astype(astype)
    return df


def words_to_snake(name: str):
    """Map Words with spaces to snake case."""
    return re.sub(r"\W+", "_", name.lower())


def camel_to_snake(name):
    """Camel case string to snake case.

    Taken from:
    https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    """
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def hash_message(message: str):
    """Get the hash of a message."""
    return hashlib.md5(bytes(message, "utf-8")).hexdigest()[:16]


def get_file_name_timestamp(url: str) -> datetime.datetime:
    """From the file url get the timestamp.

    Returns
        datetime.datetime with aware UTC localization
        If there is no timestamp found then return the current datetime.
    """
    file_name, _ = os.path.splitext(os.path.basename(url))
    date_regex = re.compile(r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])")
    if date_regex.match(file_name):
        return _process_timestamp(file_name)

    split_file_name = file_name.split("-", 1)
    if 1 < len(split_file_name) and date_regex.match(split_file_name[1]):
        return _process_timestamp(split_file_name[1])

    return datetime.datetime.now(datetime.timezone.utc)


def _process_timestamp(timestamp_str) -> datetime.datetime:
    """Get the timestamp in the file name."""
    # Windows have some non defined chars
    timestamp_str = timestamp_str.replace(u"\uf03a", ":")
    # The files in google drive have : replaced with _
    timestamp_str = timestamp_str.replace("_", ":")
    dt = datetime.datetime.fromisoformat(timestamp_str)
    if dt.tzinfo:
        dt = dt.astimezone(datetime.timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    return dt
