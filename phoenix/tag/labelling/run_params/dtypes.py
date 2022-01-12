"""Data classes for labelling run params."""
from typing import Any, Dict

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class RecalculateSFLMRunParamsURLs(base.RunParams):
    """Recalculate SFLM URLS."""

    config: Dict[str, Any]


@dataclasses.dataclass
class RecalculateSFLMRunParams(base.RunParams):
    """Recalculate SFLM Run Parameters."""

    urls: RecalculateSFLMRunParamsURLs
    general: general.GeneralRunParams
    spreadsheet_name: str
    worksheet_name: str
    tenant_folder_id: str
