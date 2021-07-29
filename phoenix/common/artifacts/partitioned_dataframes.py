"""Artifacts Partitioned DataFrame interface."""
from typing import Any, Dict

import shutil
from pathlib import Path

import awswrangler as wr
import pandas as pd
import pyarrow as pa
from dask import dataframe as dd

from phoenix.common.artifacts import dtypes


def persist(
    artifacts_dataframe_url: str, dataframe: pd.DataFrame, to_parquet_params: Dict[str, Any] = {}
) -> dtypes.ArtifactDataFrame:
    """Persist a DataFrame creating a ArtifactDataFrame.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.
            This must be a valid dataframe URL with the extension
            defined in constants.DATAFRAME_ARTIFACT_FILE_EXTENSION.
        dataframe (DataFrame): pandas DataFrame to persist.
        to_parquet_params (Dict[str, Any]): params to pass to the `to_parquet` function.

    Returns:
        ArtifactDataFrame object
    """
    _partitioned_persit(artifacts_dataframe_url, dataframe, to_parquet_params)
    artifact_dataframe = dtypes.ArtifactDataFrame(
        url=artifacts_dataframe_url, dataframe=dataframe.copy()
    )
    return artifact_dataframe


def _partitioned_persit(
    artifacts_dataframe_url: str, dataframe: pd.DataFrame, to_parquet_params: Dict[str, Any] = {}
) -> None:
    """Private persist that will be mocked when testing."""
    url = artifacts_dataframe_url
    npartitions = to_parquet_params.get("npartitions", 30)
    df = dd.from_pandas(dataframe, npartitions)
    dd.to_parquet(df, url, compute=True, **to_parquet_params)


def get(
    artifacts_dataframe_url: str, from_parquet_params: Dict[str, Any] = {}
) -> dtypes.ArtifactDataFrame:
    """Get a persisted dataframe.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.
            This must be a valid dataframe URL with the extension ".parquet"

    Returns:
        ArtifactDataFrame object
    """
    url = artifacts_dataframe_url
    ddf = dd.read_parquet(url, **from_parquet_params)
    df = ddf.compute()

    return dtypes.ArtifactDataFrame(url=artifacts_dataframe_url, dataframe=df)


def delete(artifact_dataframe: dtypes.ArtifactDataFrame) -> None:
    """Delete a persisted dataframe.

    Args:
        artifact_dataframe (ArtifactDataFrame): ArtifactDataFrame that will be deleted
    """
    url = artifact_dataframe.url
    fs_to_use, path = pa.fs.FileSystem.from_uri(url)
    if url.startswith("s3:"):
        wr.s3.delete_objects(url)
    else:
        dirpath = Path(path)
        if dirpath.exists() and dirpath.is_dir():
            shutil.rmtree(dirpath)
