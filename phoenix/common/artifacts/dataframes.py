"""Artifacts DataFrame interface."""
import functools
import os

import pandas as pd
import tentaclio

from phoenix.common import constants
from phoenix.common.artifacts import dtypes, utils


def persist(artifacts_dataframe_url: str, dataframe: pd.DataFrame) -> dtypes.ArtifactDataFrame:
    """Persist a DataFrame creating a ArtifactDataFrame.

    Args:
        artifacts_dataframe_url (str): URL for the artifact DataFrame.
            This must be a valid dataframe URL with the extension
            defined in constants.DATAFRAME_ARTIFACT_FILE_EXTENSION.
        dataframe (DataFrame): pandas DataFrame to persist.

    Returns:
        ArtifactDataFrame object
    """
    _validate_artifact_dataframe_url(artifacts_dataframe_url)
    artifact_dataframe = dtypes.ArtifactDataFrame(
        url=artifacts_dataframe_url, dataframe=dataframe.copy()
    )
    _persist(artifact_dataframe)
    return artifact_dataframe


def _persist(
    artifact_dataframe: dtypes.ArtifactDataFrame,
) -> None:
    """Private persist that will be mocked when testing."""
    url = artifact_dataframe.url
    if url.startswith("file:"):
        plain_url = url[len("file:") :]
        os.makedirs(os.path.dirname(plain_url), exist_ok=True)

    with tentaclio.open(artifact_dataframe.url, "wb") as file_io:
        artifact_dataframe.dataframe.reset_index(drop=True).to_parquet(
            file_io, index=False, compression="snappy"
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
