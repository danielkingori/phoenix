"""General Context."""
from typing import Optional

import dataclasses

from phoenix.common import artifacts, run_datetime
from phoenix.common.context import base, tenant


@dataclasses.dataclass
class GeneralContext(base.Context):
    """GeneralContext."""

    pass


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
):
    """Create for the GeneralContext."""
    if run_datetime_str:
        run_dt = run_datetime.from_file_safe_str(run_datetime_str)
    else:
        run_dt = run_datetime.create_run_datetime_now()

    tenant_config = tenant.TenantConfig(tenant_id)

    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env, tenant_config)
    return GeneralContext(art_url_reg, tenant_config, run_dt)
