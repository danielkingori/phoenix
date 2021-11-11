"""Tenant configuration."""
import dataclasses


@dataclasses.dataclass
class TenantConfig:
    """TenantConfig is the configuration of phoenix for that tenant."""

    id: str
