"""Artifact Registry Environment."""
from typing import Any, Dict
from typing_extensions import Literal

import os

from phoenix.common.artifacts import urls


Environments = Literal[
    "local",
    "production",
]

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

    raise ValueError(f"No url for environment_key: {environment_key}")


def from_env_var(variable_name: str) -> str:
    """Get the URL prefix from an environment variable."""
    result = os.getenv(variable_name)
    if result:
        return result

    raise ValueError(f"No variable set in the current system for name: {variable_name}")
