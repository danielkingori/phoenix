"""Base types."""
import dataclasses

from phoenix.common import artifacts, run_datetime
from phoenix.common.config import tenant


@dataclasses.dataclass
class RunParams:
    """Run parameters object.

    Holds the parameters during the running of a script or a notebook.
    """

    run_dt: run_datetime.RunDatetime
    tenant_config: tenant.TenantConfig
    art_url_reg: artifacts.registry.ArtifactURLRegistry
