"""Base types."""
import dataclasses

from phoenix.common import artifacts, run_datetime
from phoenix.common.context import tenant


@dataclasses.dataclass
class Context:
    """GeneralContext."""

    art_url_reg: artifacts.registry.ArtifactURLRegistry
    tenant_config: tenant.TenantConfig
    run_df: run_datetime.RunDatetime
