"""Artifact Registry."""
from typing import Any, Dict

import datetime

from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts import registry_mappers


class ArtifactURLRegistry:
    """Registry of the artifact urls."""

    environment_key: reg_env.Environments
    run_datetime: datetime.datetime
    mappers: Dict[registry_mappers.ArtifactKey, registry_mappers.ArtifactURLMapper]

    def __init__(
        self,
        run_datetime: datetime.datetime,
        environment_key: reg_env.Environments = reg_env.DEFAULT_ENVIRONMENT_KEY,
        mappers: Dict[
            registry_mappers.ArtifactKey, registry_mappers.ArtifactURLMapper
        ] = registry_mappers.DEFAULT_MAPPERS,
    ):
        """Init ArtifactURLRegistry."""
        self.environment_key = environment_key
        self.run_datetime = run_datetime
        self.mappers = mappers

    def get_run_iso_timestamp(self) -> str:
        """Get the run date."""
        return self.run_datetime.isoformat()

    def get_run_date(self) -> str:
        """Get the run date."""
        return self.run_datetime.strftime("%Y-%m-%d")

    def get_url(
        self, artifact_key: registry_mappers.ArtifactKey, url_config: Dict[str, Any] = {}
    ) -> str:
        """Get the URL for the artifact key."""
        url_config = self._build_url_config(url_config)
        for mapper_key, url_fn in self.mappers.items():
            if artifact_key == mapper_key:
                return url_fn(artifact_key, url_config, self.environment_key)

        raise ValueError(f"No url for artifact key: {artifact_key}")

    def _build_url_config(self, url_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build the url config."""
        if "RUN_ISO_TIMESTAMP" not in url_config:
            url_config["RUN_ISO_TIMESTAMP"] = self.get_run_iso_timestamp()

        if "RUN_DATE" not in url_config:
            url_config["RUN_DATE"] = self.get_run_date()

        return url_config
