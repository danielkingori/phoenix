"""Artifacts DataFrame interface."""
from typing import Any, Dict

import functools
import os

import pandas as pd
import pyarrow
import tentaclio

from phoenix.common import constants
from phoenix.common.artifacts import dtypes, utils


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
    _validate_artifact_dataframe_url(artifacts_dataframe_url)
    artifact_dataframe = dtypes.ArtifactDataFrame(
        url=artifacts_dataframe_url, dataframe=dataframe.copy()
    )
    _persist(artifact_dataframe, to_parquet_params)
    return artifact_dataframe


def _persist(
    artifact_dataframe: dtypes.ArtifactDataFrame, to_parquet_params: Dict[str, Any]
) -> None:
    """Private persist that will be mocked when testing."""
    url = artifact_dataframe.url
    if url.startswith("file:"):
        plain_url = url[len("file:") :]
        os.makedirs(os.path.dirname(plain_url), exist_ok=True)

    with tentaclio.open(artifact_dataframe.url, "wb") as file_io:
        artifact_dataframe.dataframe.reset_index(drop=True).to_parquet(
            file_io, index=False, compression="snappy", **to_parquet_params
        )


def get(
    artifacts_dataframe_url: str,
) -> dtypes.ArtifactDataFrame:
    """Get a persisted dataframe.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.
            This must be a valid dataframe URL with the extension ".parquet"

    Returns:
        ArtifactDataFrame object
    """
    _validate_artifact_dataframe_url(artifacts_dataframe_url)
    with tentaclio.open(artifacts_dataframe_url, "rb") as file_io:
        dataframe = pd.read_parquet(file_io)

    return dtypes.ArtifactDataFrame(url=artifacts_dataframe_url, dataframe=dataframe)


def delete(artifact_dataframe: dtypes.ArtifactDataFrame) -> None:
    """Delete a persisted dataframe.

    Args:
        artifact_dataframe (ArtifactDataFrame): ArtifactDataFrame that will be deleted
    """
    _validate_artifact_dataframe_url(artifact_dataframe.url)
    tentaclio.remove(artifact_dataframe.url)


def url(
    artifacts_directory: str,
    artifact_basename: str,
) -> str:
    """Create a URL for a DataFrame Artifact."""
    if not artifact_basename.endswith(constants.DATAFRAME_ARTIFACT_FILE_EXTENSION):
        artifact_basename = f"{artifact_basename}{constants.DATAFRAME_ARTIFACT_FILE_EXTENSION}"

    return f"{artifacts_directory}{artifact_basename}"


_validate_artifact_dataframe_url = functools.partial(
    utils.validate_artifact_url, constants.DATAFRAME_ARTIFACT_FILE_EXTENSION
)


def read_schema(uri: str) -> pd.DataFrame:
    """Return a Pandas dataframe corresponding to the schema of a local URI of a parquet file.

    Ref: https://stackoverflow.com/a/64288036/

    The returned dataframe has the columns: column, pa_dtype
    """
    with tentaclio.open(uri, "rb") as fb:
        schema = pyarrow.parquet.read_schema(fb, memory_map=True)

    schema = pd.DataFrame(
        (
            {"column": name, "pa_dtype": str(pa_dtype)}
            for name, pa_dtype in zip(schema.names, schema.types)
        )
    )
    schema = schema.reindex(
        columns=["column", "pa_dtype"], fill_value=pd.NA
    )  # Ensures columns in case the parquet file has an empty dataframe.
    return schema
