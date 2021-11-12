"""General RunParams."""
from typing import Optional, Tuple

import dataclasses

from phoenix.common import artifacts, run_datetime
from phoenix.common.config import tenant
from phoenix.common.run_params import base


@dataclasses.dataclass
class GeneralRunParams(base.RunParams):
    """GeneralRunParams."""

    pass


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
):
    """Create for the GeneralRunParams."""
    art_url_reg, tenant_config, run_dt = create_base_objects(
        artifact_env, tenant_id, run_datetime_str
    )
    return GeneralRunParams(run_dt=run_dt, tenant_config=tenant_config, art_url_reg=art_url_reg)


def create_base_objects(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
) -> Tuple[artifacts.registry.ArtifactURLRegistry, tenant.TenantConfig, run_datetime.RunDatetime]:
    """Create the dict that can be used to create child run params."""
    if run_datetime_str:
        run_dt = run_datetime.from_file_safe_str(run_datetime_str)
    else:
        run_dt = run_datetime.create_run_datetime_now()

    tenant_config = tenant.TenantConfig(tenant_id)

    art_url_reg = artifacts.registry.ArtifactURLRegistry(artifact_env, tenant_config, run_dt)

    return art_url_reg, tenant_config, run_dt
