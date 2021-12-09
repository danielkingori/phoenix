"""Finalise topics run params."""
from typing import Any, Dict

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class TopicsFinaliseRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    input_dataset: str
    topics: str
    tagging_final: str
    final: str


@dataclasses.dataclass
class TopicsFinaliseRunParams(base.RunParams):
    """Finalise."""

    urls: TopicsFinaliseRunParamsURLs
    general: general.GeneralRunParams
    rename_topic_to_class: bool
