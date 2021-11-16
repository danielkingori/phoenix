"""Tenant configuration."""
from typing import List, Optional

import dataclasses

import tentaclio
import yaml

from phoenix.common.artifacts import registry_environment


@dataclasses.dataclass
class TenantConfig:
    """TenantConfig is the configuration of phoenix for that tenant."""

    id: str
    google_drive_folder_id: Optional[str] = None
    crowdtangle_scrape_list_id: Optional[str] = None


def get_config_url(
    environment_key: registry_environment.Environments, file_name: str = "tenants.yaml"
) -> str:
    """Form URL to config file from environment key and file name."""
    return f"{registry_environment.config_url_prefix(environment_key)}{file_name}"


def get_configs(config_file_url: str) -> List[TenantConfig]:
    """Get list of all tenant configs."""
    with tentaclio.open(config_file_url, "r") as fh:
        processed_yaml = yaml.safe_load(fh)
    tenant_configs = [TenantConfig(**d) for d in processed_yaml["tenants"]]
    return tenant_configs


def get_config(tenant_id: str, config_file_url: str) -> TenantConfig:
    """Get config for tenant ID."""
    tenant_configs = get_configs(config_file_url)
    tenant_config = [config for config in tenant_configs if config.id == tenant_id]
    if len(tenant_config) > 1:
        raise ValueError(f"Multiples tenants found for [tenant_id={tenant_id}].")
    if len(tenant_config) == 0:
        raise ValueError(f"No tenant found for [tenant_id={tenant_id}].")
    return tenant_config[0]
