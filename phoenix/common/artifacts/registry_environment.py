"""Artifact Registry Environment."""
from typing import Any, Dict

from phoenix.common.artifacts import urls


DEFAULT_ENVIRONMENT_KEY = "local"


def default_url_prefix(
    artifact_key: str, url_config: Dict[str, Any], environment_key: str = DEFAULT_ENVIRONMENT_KEY
):
    """URL prefix for static artifacts."""
    if environment_key == DEFAULT_ENVIRONMENT_KEY:
        return f"{urls.get_local()}"

    raise ValueError(f"No url for environment_key: {environment_key}")
