"""Functionality for processing file names."""
from typing import Optional

import dataclasses
import datetime
import os
import re

from phoenix.common import run_datetime


@dataclasses.dataclass
class SourceFileName:
    """SourceFileName."""

    is_legacy: bool
    full_url: str
    folder_url: str
    file_name_prefix: Optional[str]
    run_dt: run_datetime.RunDatetime
    extension: str


def get_source_file_name(url: str) -> Optional[SourceFileName]:
    """Get the source file name object from the URL."""
    source_file_name = get_legacy_source_file_name(url)
    if source_file_name:
        return source_file_name

    return None


def get_legacy_source_file_name(url: str) -> Optional[SourceFileName]:
    """Get SourceFileName for legacy URL."""
    folder_url = os.path.dirname(url)
    file_name, extension = os.path.splitext(os.path.basename(url))
    date_regex = re.compile(r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])T")
    dt = None
    if date_regex.match(file_name):
        dt = _process_legacy_timestamp(file_name)
        return SourceFileName(
            is_legacy=True,
            full_url=url,
            folder_url=folder_url,
            extension=extension,
            file_name_prefix=None,
            run_dt=run_datetime.RunDatetime(dt),
        )

    split_file_name = file_name.split("-", 1)
    if 1 < len(split_file_name) and date_regex.match(split_file_name[1]):
        dt = _process_legacy_timestamp(split_file_name[1])

    file_name_prefix = None
    if split_file_name[0]:
        file_name_prefix = f"{split_file_name[0]}-"

    if dt:
        return SourceFileName(
            is_legacy=True,
            full_url=url,
            folder_url=folder_url,
            extension=extension,
            file_name_prefix=file_name_prefix,
            run_dt=run_datetime.RunDatetime(dt),
        )

    return None


def _process_legacy_timestamp(timestamp_str) -> datetime.datetime:
    """Get the timestamp in the file name."""
    # Windows have some non defined chars
    timestamp_str = timestamp_str.replace("\uf03a", ":")
    # The files in google drive have : replaced with _
    timestamp_str = timestamp_str.replace("_", ":")
    dt = datetime.datetime.fromisoformat(timestamp_str)
    if dt.tzinfo:
        dt = dt.astimezone(datetime.timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    return dt
