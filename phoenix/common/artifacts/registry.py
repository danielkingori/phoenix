"""Artifact Registry."""
from typing import Any, Dict, Optional

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts import registry_mappers
from phoenix.common.config import tenant


class ArtifactURLRegistry:
    """Registry of the artifact urls."""

    environment_key: reg_env.Environments
    tenant_config: tenant.TenantConfig
    run_dt: run_datetime.RunDatetime
    mappers: registry_mappers.MapperDict

    def __init__(
        self,
        environment_key: reg_env.Environments,
        tenant_config: tenant.TenantConfig,
        run_dt: run_datetime.RunDatetime,
        mappers: Optional[registry_mappers.MapperDict] = None,
    ):
        """Init ArtifactURLRegistry."""
        self.environment_key = environment_key
        self.tenant_config = tenant_config
        self.run_dt = run_dt
        # The default mappers are set via an initialisation rather then as a default
        # parameter to help with the automatic reloading of changes, during a notebook session
        # that are made to default mappers.
        if not mappers:
            mappers = registry_mappers.get_default_mappers()

        self.mappers = mappers

    def get_url(
        self, artifact_key: registry_mappers.ArtifactKey, url_config: Dict[str, Any] = {}
    ) -> str:
        """Get the URL for the artifact key."""
        url_config = self._build_url_config(url_config)
        for mapper_key, url_fn in self.mappers.items():
            if artifact_key == mapper_key:
                return url_fn(artifact_key, url_config, self.environment_key, self.tenant_config)

        raise ValueError(f"No url for artifact key: {artifact_key}")

    def _build_url_config(self, url_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build the url config."""
        if "RUN_DATETIME" not in url_config:
            url_config["RUN_DATETIME"] = self.run_dt.to_file_safe_str()

        if "RUN_DATE" not in url_config:
            url_config["RUN_DATE"] = self.run_dt.to_run_date_str()

        return url_config
