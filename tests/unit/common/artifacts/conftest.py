"""Conftest artifacts."""
import pytest

from phoenix.common.config import tenant


@pytest.fixture
def tenant_config():
    """Create a tenant config for testing."""
    return tenant.TenantConfig(id="test_tenant")
