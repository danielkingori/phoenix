"""Conftest for artifacts."""
from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment


def create_test_art_url_reg(environment_key: registry_environment.Environments = "local"):
    """Create a test ArtifactURLRegistry."""
    run_dt = run_datetime.create_run_datetime_now()
    return registry.ArtifactURLRegistry(environment_key, run_dt)
