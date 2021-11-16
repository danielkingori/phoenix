"""Tenant configuration."""
from typing import List, Optional

import dataclasses

import tentaclio
import yaml


@dataclasses.dataclass
class TenantConfig:
    """TenantConfig is the configuration of phoenix for that tenant."""

    id: str
    google_drive_folder_id: Optional[str] = None
    crowdtangle_scrape_list_id: Optional[str] = None


def get_configs(config_url: str) -> List[TenantConfig]:
    """Get list of all tenant configs."""
    file_url = config_url + "tenants.yaml"
    with tentaclio.open(file_url, "r") as fh:
        processed_yaml = yaml.safe_load(fh)
    tenant_configs = [TenantConfig(**d) for d in processed_yaml["tenants"]]
    return tenant_configs


def get_config(tenant_id: str, config_url: str) -> TenantConfig:
    """Get config for tenant ID."""
    tenant_configs = get_configs(config_url)
    tenant_config = [config for config in tenant_configs if config.id == tenant_id]
    if len(tenant_config) > 1:
        raise ValueError(f"Multiples tenants found for [tenant_id={tenant_id}].")
    if len(tenant_config) == 0:
        raise ValueError(f"No tenant found for [tenant_id={tenant_id}].")
    return tenant_config[0]
