"""Data classes for finalise run params."""
from typing import Any, Dict, Union

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class DataPullRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    input_dataset: str
    pulled: str
    for_tagging: str


@dataclasses.dataclass
class DataPullRunParams(base.RunParams):
    """DataPull."""

    urls: DataPullRunParamsURLs
    general: general.GeneralRunParams
    year_filter: Union[int, None]
    month_filter: Union[int, None]
    include_all_data_for_month: bool
