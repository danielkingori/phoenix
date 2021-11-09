"""Artifact Registry."""
from typing import Any, Dict, Optional

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts import registry_mappers


class ArtifactURLRegistry:
    """Registry of the artifact urls."""

    tenant_id: str
    environment_key: reg_env.Environments
    run_dt: run_datetime.RunDatetime
    mappers: registry_mappers.MapperDict

    def __init__(
        self,
        tenant_id: str,
        run_dt: run_datetime.RunDatetime,
        environment_key: reg_env.Environments = reg_env.DEFAULT_ENVIRONMENT_KEY,
        mappers: Optional[registry_mappers.MapperDict] = None,
    ):
        """Init ArtifactURLRegistry."""
        self.tenant_id = tenant_id
        self.environment_key = environment_key
        self.run_dt = run_dt
        if not self.tenant_id:
            raise ValueError(f"Invalid Tenant ID: {tenant_id}")
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
                return url_fn(artifact_key, url_config, self.environment_key)

        raise ValueError(f"No url for artifact key: {artifact_key}")

    def _build_url_config(self, url_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build the url config."""
        if "RUN_DATETIME" not in url_config:
            url_config["RUN_DATETIME"] = self.run_dt.to_file_safe_str()

        if "RUN_DATE" not in url_config:
            url_config["RUN_DATE"] = self.run_dt.to_run_date_str()

        if "TENANT_ID" not in url_config:
            url_config["TENANT_ID"] = self.tenant_id

        return url_config
