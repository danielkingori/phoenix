"""Utils for pulling data."""
import datetime
import hashlib
import re

import pandas as pd

from phoenix.common.artifacts import source_file_name_processing


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
