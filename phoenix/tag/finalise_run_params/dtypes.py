"""Data classes for finalise run params."""
from typing import Any, Dict

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class FinaliseRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    input_dataset: str
    objects_tensions: str
    language_sentiment_objects: str
    tagging_final: str
    final: str


@dataclasses.dataclass
class FinaliseRunParams(base.RunParams):
    """Finalise."""

    urls: FinaliseRunParamsURLs
    general: general.GeneralRunParams
    include_objects_tensions: bool
    include_sentiment: bool
