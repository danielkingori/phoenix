"""Complete sentiment RunParams."""
from typing import Any, Dict, Optional

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class CompleteSentimentRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    async_job_group: str
    language_sentiment_objects: str


@dataclasses.dataclass
class CompleteSentimentRunParams(base.RunParams):
    """CompleteSentiment."""

    urls: CompleteSentimentRunParamsURLs
    general: general.GeneralRunParams


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
) -> CompleteSentimentRunParams:
    """Create the CompleteSentimentRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(general_run_params, object_type, year_filter, month_filter)
    return CompleteSentimentRunParams(
        general=general_run_params,
        urls=urls,
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
    object_type: str,
    year_filter: int,
    month_filter: int,
) -> CompleteSentimentRunParamsURLs:
    """Get CompleteSentimentRunParamsURLs."""
    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    return CompleteSentimentRunParamsURLs(
        config=url_config,
        async_job_group=art_url_reg.get_url("tagging_runs-async_job_group", url_config),
        language_sentiment_objects=art_url_reg.get_url(
            "tagging_runs-language_sentiment_objects", url_config
        ),
    )
