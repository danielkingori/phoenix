"""Artifacts Partitioned DataFrame interface."""
from typing import Any, Dict, List

import shutil
from pathlib import Path

import awswrangler as wr
import pandas as pd
import pyarrow as pa
from dask import dataframe as dd

from phoenix.common.artifacts import dtypes


def persist(
    artifacts_dataframe_url: str,
    dataframe: pd.DataFrame,
    partition_cols: List[str],
    npartitions: int = 1,
    to_parquet_params: Dict[str, Any] = {},
) -> dtypes.ArtifactDataFrame:
    """Persist a DataFrame as a partitioned parquet creating a ArtifactDataFrame.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.
        dataframe (DataFrame): pandas DataFrame to persist.
        partition_cols (List[str]): list of columns to partition on.
        npartitions (int): number of npartitions for `from_pandas`, default 1:
            https://docs.dask.org/en/latest/generated/dask.dataframe.from_pandas.html
        to_parquet_params: aux params to pass to the `to_parquet` function.

    Returns:
        ArtifactDataFrame object
    """
    _partitioned_persit(
        artifacts_dataframe_url, dataframe, partition_cols, npartitions, to_parquet_params
    )
    artifact_dataframe = dtypes.ArtifactDataFrame(
        url=artifacts_dataframe_url, dataframe=dataframe.copy()
    )
    return artifact_dataframe


def _partitioned_persit(
    artifacts_dataframe_url: str,
    dataframe: pd.DataFrame,
    partition_cols: List[str],
    npartitions: int = 1,
    to_parquet_params: Dict[str, Any] = {},
) -> None:
    """Private persist that will be mocked when testing."""
    url = artifacts_dataframe_url
    df = dd.from_pandas(dataframe, npartitions)
    dd.to_parquet(df, url, compute=True, partition_cols=partition_cols, **to_parquet_params)


def get(
    artifacts_dataframe_url: str, from_parquet_params: Dict[str, Any] = {}
) -> dtypes.ArtifactDataFrame:
    """Get a persisted partitioned dataframe.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.

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
