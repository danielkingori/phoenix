"""Artifacts Json interface."""
from typing import Any, Dict, List, Union

import copy
import functools
import json
import os

import tentaclio

from phoenix.common.artifacts import dtypes, utils


VALID_SUFFIX = ".json"


def persist(artifacts_json_url: str, obj: Union[List, Dict[Any, Any]]) -> dtypes.ArtifactJson:
    """Persist a obj creating a ArtifactJson.

    Ecoding is "utf-8" which comes from tentaclio.
    Args:
        artifacts_json_url (str): URL for the artifact.
            This must be a valid artifact URL with the extension
        obj (List|Dict): dictionary

    Returns:
        ArtifactJson object
    """
    _validate_artifact_json_url(artifacts_json_url)
    artifact_json = dtypes.ArtifactJson(url=artifacts_json_url, obj=copy.deepcopy(obj))
    _persist(artifact_json)
    return artifact_json


def _persist(
    artifact_json: dtypes.ArtifactJson,
) -> None:
    """Private persist that will be mocked when testing."""
    url = artifact_json.url
    if url.startswith("file:"):
        plain_url = url[len("file:") :]
        os.makedirs(os.path.dirname(plain_url), exist_ok=True)

    with tentaclio.open(artifact_json.url, "w") as file_io:
        json.dump(artifact_json.obj, file_io)


def get(
    artifacts_json_url: str,
) -> dtypes.ArtifactJson:
    """Get a persisted dataframe.

    Encoding is "utf-8" which comes from tentaclio.

    Args:
        artifacts_json_url (str): URL for the artifact.

    Returns:
        ArtifactJson object
    """
    _validate_artifact_json_url(artifacts_json_url)
    with tentaclio.open(artifacts_json_url, "r") as file_io:
        obj = json.load(file_io)

    return dtypes.ArtifactJson(url=artifacts_json_url, obj=obj)


def url(
    artifacts_directory: str,
    artifact_basename: str,
) -> str:
    """Create a URL for a Artifact."""
    if not artifact_basename.endswith(VALID_SUFFIX):
        artifact_basename = f"{artifact_basename}{VALID_SUFFIX}"

    return f"{artifacts_directory}{artifact_basename}"


_validate_artifact_json_url = functools.partial(utils.validate_artifact_url, VALID_SUFFIX)
