"""Artifact Registry Environment."""
from typing import Any, Dict, Optional, Union
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
PRODUCTION_DASHBOARD_ENV_VAR_KEY = "PRODUCTION_DASHBOARD_URL_PREFIX"


def default_url_prefix(
    artifact_key: str, url_config: Dict[str, Any], environment_key: Environments, tenant_id: str
):
    """URL prefix for static artifacts."""
    base_url = _get_url_from_environment_key(environment_key)
    return f"{base_url}{tenant_id}/"


def _get_url_from_environment_key(environment_key: Environments):
    """Get URL from the environment_key."""
    if environment_key == DEFAULT_ENVIRONMENT_KEY:
        return f"{urls.get_local()}"

    if environment_key == "production":
        return from_env_var(PRODUCTION_ENV_VAR_KEY)

    valid_url = valid_cloud_storage_url(environment_key)
    if valid_url:
        return valid_url

    raise ValueError(f"No url for environment_key: {environment_key}")


def from_env_var(variable_name: str, raise_if_not_found=True) -> Optional[str]:
    """Get the URL prefix from an env variable.

    Arguments:
        variable_name (str): the name of the env to get URL prefix.
        raise_if_not_found (bool): suppress the exception if the env variable is not set

    Raises:
        ValueError: If the env variable is set as if not valid.
            This occurs even if the raise_if_not_found is True.

    Returns:
        URL prefix as a string or None.
    """
    result = os.getenv(variable_name)
    if result:
        valid_cloud_storage_url(result, variable_name)
        return result

    if raise_if_not_found:
        raise ValueError(f"No variable set in the current system for name: {variable_name}")

    return None


def valid_cloud_storage_url(url: str, env_variable_name: Optional[str] = None) -> str:
    """Check is the url is a valid url."""
    parsed_url = urllib.parse.urlparse(url)
    error_suffix = ""
    if env_variable_name:
        error_suffix = f"For environment variable: {env_variable_name}."
    if parsed_url.scheme not in VALID_ENVIRONMENT_SCHEMAS:
        raise ValueError(
            (
                f"Environment is not a valid url: {url}."
                f" Please use URLs with schemas: {VALID_ENVIRONMENT_SCHEMAS}"
                f" {error_suffix}"
            )
        )

    if not os.path.isdir(parsed_url.path):
        raise ValueError(
            (
                f"Environment is not a valid url directory: {url}."
                f" Please add a / and make sure the URL is a directory."
                f" {error_suffix}"
            )
        )

    return url


def dashboard_url_prefix(
    artifact_key: str, url_config: Dict[str, Any], environment_key: Environments, tenant_id: str
):
    """URL prefix for public dashboard.

    This is an extra URL prefix that is only used for dashboard artifacts.
    Dashboard artifacts are stored in an other cloud point as they have different
    permissions.

    Default is to return None.
    Production will use the env variable set in PRODUCTION_DASHBOARD_ENV_VAR_KEY.
    See phoenix/common/artifacts/registry_environment.py.
    """
    url = _get_dashboard_url_from_environment(environment_key)
    if not url:
        return url
    return f"{url}{tenant_id}/"


def _get_dashboard_url_from_environment(environment_key: Environments):
    """Get the dashboard URL from the environment."""
    if environment_key == DEFAULT_ENVIRONMENT_KEY:
        return None

    if environment_key == "production":
        return from_env_var(PRODUCTION_DASHBOARD_ENV_VAR_KEY, raise_if_not_found=False)

    raise ValueError(
        (
            f"No url for environment_key: {environment_key}."
            " Be aware that the dashboard URL prefix must be set with an environment variable."
        )
    )
