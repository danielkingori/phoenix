"""Default URL mapper."""
from typing import Any, Dict, Protocol

from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts.registry_mappers import artifact_keys
from phoenix.common.config import tenant


class ArtifactURLMapper(Protocol):
    """Protocol for the artifactURLMapper."""

    def __call__(
        self,
        artifact_key: artifact_keys.ArtifactKey,
        url_config: Dict[str, Any],
        environment_key: reg_env.Environments,
        tenant_config: tenant.TenantConfig,
    ) -> str:
        """Protocol for the artifactURLMapper."""
        ...


def url_mapper(
    format_str: str,
    artifact_key: artifact_keys.ArtifactKey,
    url_config: Dict[str, Any],
    environment_key: reg_env.Environments,
    tenant_config: tenant.TenantConfig,
):
    """Generalised url mapper."""
    prefix = reg_env.default_url_prefix(
        artifact_key, url_config, environment_key, tenant_config.id
    )
    url_str_formated = format_str.format(**url_config)
    return f"{prefix}{url_str_formated}"


MapperDict = Dict[artifact_keys.ArtifactKey, ArtifactURLMapper]
