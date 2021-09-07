"""Artifact Registry Environment."""
from typing import Any, Dict, Union
from typing_extensions import Literal

import os
import urllib

from phoenix.common.artifacts import urls


Environments = Union[
    # This string must be a valid URL to a cloud storage provider
    str,
    Literal[
        "local",
        "production",
    ],
]

VALID_ENVIRONMENT_SCHEMAS = ["file", "s3", "gcp"]

DEFAULT_ENVIRONMENT_KEY: Environments = "local"
# The ENV variable name that will set the prefix when in production
PRODUCTION_ENV_VAR_KEY = "PRODUCTION_ARTIFACTS_URL_PREFIX"


def default_url_prefix(
    artifact_key: str, url_config: Dict[str, Any], environment_key: str = DEFAULT_ENVIRONMENT_KEY
):
    """URL prefix for static artifacts."""
    if environment_key == DEFAULT_ENVIRONMENT_KEY:
        return f"{urls.get_local()}"

    if environment_key == "production":
        return from_env_var(PRODUCTION_ENV_VAR_KEY)

    valid_url = valid_cloud_storage_url(environment_key)
    if valid_url:
        return valid_url

    raise ValueError(f"No url for environment_key: {environment_key}")


def from_env_var(variable_name: str) -> str:
    """Get the URL prefix from an environment variable."""
    result = os.getenv(variable_name)
    if result:
        return result

    raise ValueError(f"No variable set in the current system for name: {variable_name}")


def valid_cloud_storage_url(url: str) -> str:
    """Check is the url is a valid url."""
    parsed_url = urllib.parse.urlparse(url)
    if parsed_url.scheme not in VALID_ENVIRONMENT_SCHEMAS:
        raise ValueError(
            (
                f"Environment is not a valid url: {url}."
                f" Please use URLs with schemas: {VALID_ENVIRONMENT_SCHEMAS}"
            )
        )

    if not os.path.isdir(parsed_url.path):
        raise ValueError(
            (
                f"Environment is not a valid url directory: {url}."
                f" Please add a / and make sure the URL is a directory."
            )
        )

    return url
