"""Artifact Registry."""
from typing import Any, Dict

import datetime

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


class ArtifactURLRegistry:
    """Registry of the artifact urls."""

    environment_key: str
    run_datetime: datetime.datetime
    mappers = {"source-": source_url}

    def __init__(
        self, run_datetime: datetime.datetime, environment_key: str = DEFAULT_ENVIRONMENT_KEY
    ):
        """Init ArtifactURLRegistry."""
        self.environment_key = environment_key
        self.run_datetime = run_datetime

    def get_url(self, artifact_key: str, url_config: Dict[str, Any] = {}) -> str:
        """Get the URL for the artifact key."""
        url_config = self._build_url_config(url_config)
        for mapper_prefix, url_fn in self.mappers.items():
            if artifact_key.startswith(mapper_prefix):
                return url_fn(artifact_key, url_config, self.environment_key)

        raise ValueError(f"No url for artifact key: {artifact_key}")

    def _build_url_config(self, url_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build the url config."""
        if "RUN_ISO_TIMESTAMP" not in url_config:
            url_config["RUN_ISO_TIMESTAMP"] = self.run_datetime.isoformat()

        if "RUN_DATE" not in url_config:
            url_config["RUN_DATE"] = self.run_datetime.strftime("%Y-%m-%d")

        return url_config
