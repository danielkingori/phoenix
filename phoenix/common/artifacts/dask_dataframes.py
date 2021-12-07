"""Artifacts Dask DataFrame interface."""
from typing import Any, Dict, List

import shutil
from pathlib import Path
from urllib import parse

import boto3
from dask import dataframe as dd

from phoenix.common.artifacts import dtypes


def persist(
    artifacts_dataframe_url: str,
    dataframe: dd.DataFrame,
    partition_cols: List[str],
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
    ddf = dd.read_parquet(url, **from_parquet_params)

    return dtypes.ArtifactDaskDataFrame(url=artifacts_dataframe_url, dataframe=ddf)


def delete(artifact_dataframe: dtypes.ArtifactDaskDataFrame) -> None:
    """Delete a persisted dataframe.

    Args:
        artifact_dataframe (ArtifactDaskDataFrame): ArtifactDaskDataFrame that will be deleted
    """
    url = artifact_dataframe.url
    url_parsed = parse.urlparse(url)
    if url.startswith("s3:"):
        return _delete_s3(url_parsed)
    else:
        dirpath = Path(url_parsed.path)
        if dirpath.exists() and dirpath.is_dir():
            shutil.rmtree(dirpath)


def _delete_s3(url) -> None:
    """Delete a folder or object from s3."""
    bucket_name = url.hostname
    key_name = url.path[1:] if url.path != "" else ""
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    for key in bucket.objects.filter(Prefix=key_name):
        key.delete()
