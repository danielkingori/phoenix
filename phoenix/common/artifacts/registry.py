"""Artifact Registry."""
from typing import Any, Dict

from phoenix.common.artifacts import urls


DEFAULT_ENVIRONMENT_KEY = "local"


def source_url(
    artifact_key: str, url_config: Dict[str, Any], environment_key: str = DEFAULT_ENVIRONMENT_KEY
) -> str:
    """Get a source URL."""
    required_url_config = ["RUN_DATE", "RUN_ISO_TIMESTAMP"]
    url_config_keys = list(url_config.keys())
    if not all(item in url_config_keys for item in required_url_config):
        raise ValueError(
            "Invalid URL config."
            f"Expecting keys: {required_url_config}. Got keys: {url_config_keys}"
        )

    if environment_key == DEFAULT_ENVIRONMENT_KEY:
        prefix = f"{urls.get_local()}{url_config['RUN_DATE']}"
    else:
        raise ValueError(f"No url for artifact key: {artifact_key}")

    mapping = {"posts": f"/source_runs/source-posts-{url_config['RUN_ISO_TIMESTAMP']}.json"}

    for mapper_suffix, url_str in mapping.items():
        if artifact_key.endswith(mapper_suffix):
            return f"{prefix}{url_str}"

    raise ValueError(f"No url for artifact key: {artifact_key}")
