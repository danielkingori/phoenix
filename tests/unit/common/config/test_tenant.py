"""Test Tenant configuration."""

from phoenix.common.config import tenant


def test_tenant_config():
    """Test TenantConfig."""
    id_str = "id"
    tenant_config = tenant.TenantConfig(id_str)
    assert tenant_config.id == id_str
