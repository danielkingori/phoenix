"""Artifacts Dask DataFrame interface."""
from typing import Any, Dict

import shutil
from pathlib import Path

import awswrangler as wr
import pyarrow as pa
from dask import dataframe as dd

from phoenix.common.artifacts import dtypes


def persist(
    artifacts_dataframe_url: str,
    dataframe: dd.DataFrame,
    to_parquet_params: Dict[str, Any] = {},
) -> dtypes.ArtifactDaskDataFrame:
    """Persist a DataFrame as a partitioned parquet creating a ArtifactDataFrame.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.
        dataframe (DataFrame): pandas DataFrame to persist.
        to_parquet_params: aux params to pass to the `to_parquet` function.

    Returns:
        ArtifactDaskDataFrame object
    """
    _persist(artifacts_dataframe_url, dataframe, to_parquet_params)
    artifact_dataframe = dtypes.ArtifactDaskDataFrame(
        url=artifacts_dataframe_url, dataframe=dataframe.copy()
    )
    return artifact_dataframe


def _persist(
    artifacts_dataframe_url: str,
    ddf: dd.DataFrame,
    to_parquet_params: Dict[str, Any] = {},
) -> None:
    """Private persist that will be mocked when testing."""
    url = artifacts_dataframe_url
    dd.to_parquet(ddf, url, **to_parquet_params)


def get(
    artifacts_dataframe_url: str, from_parquet_params: Dict[str, Any] = {}
) -> dtypes.ArtifactDaskDataFrame:
    """Get a persisted dask dataframe.

    Args:
        artifacts_dataframe_url (str): URL for the artifact Dask DataFrame.

    Returns:
        ArtifactDaskDataFrame object
    """
    url = artifacts_dataframe_url
    try:
        ddf = dd.read_parquet(url, **from_parquet_params)
    except ValueError as e:
        not_found_error = (
            "No files satisfy the `require_extension` criteria"
            " (files must end with ('.parq', '.parquet'))."
        )
        if not_found_error in str(e):
            raise FileNotFoundError(e)

    return dtypes.ArtifactDaskDataFrame(url=artifacts_dataframe_url, dataframe=ddf)


def delete(artifact_dataframe: dtypes.ArtifactDaskDataFrame) -> None:
    """Delete a persisted dataframe.

    Args:
        artifact_dataframe (ArtifactDaskDataFrame): ArtifactDaskDataFrame that will be deleted
    """
    url = artifact_dataframe.url
    fs_to_use, path = pa.fs.FileSystem.from_uri(url)
    if url.startswith("s3:"):
        wr.s3.delete_objects(url)
    else:
        dirpath = Path(path)
        if dirpath.exists() and dirpath.is_dir():
            shutil.rmtree(dirpath)
