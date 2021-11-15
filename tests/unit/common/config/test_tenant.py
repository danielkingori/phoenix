"""Test Tenant configuration."""

from phoenix.common.config import tenant


def test_tenant_config():
    """Test TenantConfig."""
    id_str = "id"
    tenant_config = tenant.TenantConfig(id_str)
    assert tenant_config.id == id_str


def test_get_tenant_configs():
    """Test getting tenant configs.

    Note this uses the template yaml, which is good because it ensures the template that users see
    matches the interface and functionality.
    """
    expected_tenant_configs = [
        tenant.TenantConfig(
            "tenant_id_1",
            google_drive_folder_id="folder_id_1",
            crowdtangle_scrape_list_id="list_id_1",
        ),
        tenant.TenantConfig("tenant_id_2", crowdtangle_scrape_list_id="list_id_2"),
        tenant.TenantConfig(
            "tenant_id_3",
            google_drive_folder_id="folder_id_3",
        ),
    ]

    out_tenant_configs = tenant.get_configs(file_name="tenants_template.yaml")
    for expected, out in zip(expected_tenant_configs, out_tenant_configs):
        assert expected == out
