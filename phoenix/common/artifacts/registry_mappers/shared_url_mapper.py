"""URL mappers that are used by different mappers."""
from typing import Any, Dict

from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts.registry_mappers import artifact_keys
from phoenix.common.context import tenant


def dashboard_url_mapper(
    format_str: str,
    artifact_key: artifact_keys.ArtifactKey,
    url_config: Dict[str, Any],
    environment_key: reg_env.Environments,
    tenant_config: tenant.TenantConfig,
):
    """Dashboard url mapper.

    The dashboard artifacts are saved into a separate bucket with different permissions.
    As such they return None unless the env variable is set and the environment_key is production.
    See phoenix/common/artifacts/registry_environment.py::dashboard_url_prefix
    for more information.
    """
    prefix = reg_env.dashboard_url_prefix(artifact_key, url_config, environment_key)
    if not prefix:
        return None
    url_str_formated = format_str.format(**url_config)
    return f"{prefix}{url_str_formated}"
