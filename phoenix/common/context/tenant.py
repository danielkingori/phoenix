"""Tenant Config for the context."""
import dataclasses


@dataclasses.dataclass
class TenantConfig:
    """TenantConfiguration."""

    id: str
