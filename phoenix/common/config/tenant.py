"""Tenant configuration."""
from typing import List, Optional

import dataclasses

import tentaclio
import yaml

from phoenix.common import artifacts


@dataclasses.dataclass
class TenantConfig:
    """TenantConfig is the configuration of phoenix for that tenant."""

    id: str
    google_drive_folder_id: Optional[str] = None
    crowdtangle_scrape_list_id: Optional[str] = None


def get_configs(
    file_name: str = "tenants.yaml", file_dir: Optional[str] = None
) -> List[TenantConfig]:
    """Get list of all tenant configs."""
    if file_dir is None:
        file_dir = artifacts.urls.get_static_config()
    yaml_path = file_dir + file_name
    with tentaclio.open(yaml_path, "r") as fh:
        processed_yaml = yaml.safe_load(fh)
    tenant_configs = [TenantConfig(**d) for d in processed_yaml["tenants"]]
    return tenant_configs
