"""Conftest for artifacts."""
from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment
from phoenix.common.config import tenant


def create_test_art_url_reg(environment_key: registry_environment.Environments = "local"):
    """Create a test ArtifactURLRegistry."""
    run_dt = run_datetime.create_run_datetime_now()
    tenant_config = tenant.TenantConfig(id="test")
    return registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt)
