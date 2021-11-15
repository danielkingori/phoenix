"""Tenant configuration."""
from typing import Optional

import dataclasses


@dataclasses.dataclass
class TenantConfig:
    """TenantConfig is the configuration of phoenix for that tenant."""

    id: str
    google_drive_folder_id: Optional[str] = None
    crowdtangle_scrape_list_id: Optional[str] = None
