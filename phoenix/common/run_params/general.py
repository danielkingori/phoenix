"""General RunParams."""
from typing import Optional

import dataclasses

from phoenix.common import artifacts, run_datetime
from phoenix.common.config import tenant
from phoenix.common.run_params import base


@dataclasses.dataclass
class GeneralRunParams(base.RunParams):
    """GeneralRunParams."""

    run_dt: run_datetime.RunDatetime
    tenant_config: tenant.TenantConfig
    art_url_reg: artifacts.registry.ArtifactURLRegistry


def create(
    environment_key: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
):
    """Create for the GeneralRunParams."""
    if run_datetime_str:
        run_dt = run_datetime.from_file_safe_str(run_datetime_str)
    else:
        run_dt = run_datetime.create_run_datetime_now()

    tenant_config = tenant.get_config(tenant_id, tenant.get_config_url(environment_key))

    art_url_reg = artifacts.registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt)
    return GeneralRunParams(run_dt=run_dt, tenant_config=tenant_config, art_url_reg=art_url_reg)
