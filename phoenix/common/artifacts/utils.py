"""Utils for artifacts module."""
from typing import Optional

import os

import pandas as pd
import tentaclio
from dask import dataframe as dd


def validate_artifact_url(
    suffix: str,
    artifacts_url: str,
) -> bool:
    """Validate an URL for Artifact.

    Raises:
        ValueError if invalid.
    """
    if not artifacts_url.endswith(suffix):
        url_msg = f"URL: {artifacts_url}"
        invalid_msg = f"is not valid must end with {suffix}"
        raise ValueError(f"{url_msg} {invalid_msg}")
    return True


def pandas_to_dask(df: pd.DataFrame, npartitions: Optional[int] = None) -> dd.DataFrame:
    """Utility function to convert pandas dataframe to dask dataframe.

    Arguments:
        df: pd.DataFrame, dataframe to convert
        npartitions: int, number of partitions to split the pandas into.
            Default behaviour is to to use environment variable DEFAULT_NPARTITIONS.
            DEFAULT_NPARTITIONS should be set to number of workers.

    TODO: at some point there should be a more complete partition algorithm see:
        https://gitlab.com/howtobuildup/phoenix/-/issues/46

    Returns:
        dd.DataFrame, dask dataframe
    """
    if not npartitions:
        npartitions = int(os.getenv("DEFAULT_NPARTITIONS", 1))

    return dd.from_pandas(df, npartitions=npartitions)


def create_folders_if_needed(url):
    """Create the folders for a URL if needed.

    For local URLs we have to create the folders.
    """
    parsed_url = tentaclio.urls.URL(url)
    if parsed_url.scheme == "file":
        os.makedirs(os.path.dirname(parsed_url.path), exist_ok=True)


def copy(from_url, to_url):
    """Copy file from one URL to another."""
    create_folders_if_needed(to_url)
    tentaclio.copy(from_url, to_url)
