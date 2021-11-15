"""Test Tenant configuration."""
import pytest

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


def test_get_tenant_config():
    """Test getting tenant config for tenant ID."""
    expected_config = tenant.TenantConfig(
        "tenant_id_3",
        google_drive_folder_id="folder_id_3",
    )
    assert expected_config == tenant.get_config("tenant_id_3", file_name="tenants_template.yaml")


def test_get_tenant_config_raises_for_tenant_not_found(tmp_path):
    """Test getting tenant config raises for not found tenant ID."""
    raw_yaml = """---
    tenants:
        -
            id: tenant_id_1
    """
    path = tmp_path / "test.yaml"
    path.write_text(raw_yaml)
    with pytest.raises(ValueError):
        tenant.get_config("tenant_id_2", file_name=str(path), file_dir="")


def test_get_tenant_config_raises_for_multiple_tenants_found(tmp_path):
    """Test getting tenant config raises for multiple tenants found for ID."""
    raw_yaml = """---
    tenants:
        -
            id: tenant_id_1
        -
            id: tenant_id_1
    """
    path = tmp_path / "test.yaml"
    path.write_text(raw_yaml)
    with pytest.raises(ValueError):
        tenant.get_config("tenant_id_1", file_name=str(path), file_dir="")
