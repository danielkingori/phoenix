"""Start sentiment RunParams."""
from typing import Any, Dict, Optional

import dataclasses
import os

from phoenix.common.run_params import base, general


AWS_DATA_ACCESS_ROLE_ENV_KEY = "AWS_DATA_ACCESS_ROLE"


@dataclasses.dataclass
class StartSentimentRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    objects: str
    async_job_group: str
    comprehend_base: str


@dataclasses.dataclass
class StartSentimentRunParams(base.RunParams):
    """StartSentiment."""

    urls: StartSentimentRunParamsURLs
    general: general.GeneralRunParams
    aws_data_access_role: str


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
    aws_data_access_role: Optional[str],
) -> StartSentimentRunParams:
    """Create the StartSentimentRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(general_run_params, object_type, year_filter, month_filter)
    return StartSentimentRunParams(
        general=general_run_params,
        urls=urls,
        aws_data_access_role=_get_aws_data_access_role(aws_data_access_role),
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
    object_type: str,
    year_filter: int,
    month_filter: int,
) -> StartSentimentRunParamsURLs:
    """Get StartSentimentRunParamsURLs."""
    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    return StartSentimentRunParamsURLs(
        config=url_config,
        objects=art_url_reg.get_url("tagging_runs-objects", url_config),
        async_job_group=art_url_reg.get_url("tagging_runs-async_job_group", url_config),
        comprehend_base=art_url_reg.get_url("tagging_runs-comprehend_base", url_config),
    )


def _get_aws_data_access_role(given_aws_data_access_role: Optional[str]) -> str:
    """Get the aws_data_access_role from given argument or environment."""
    if given_aws_data_access_role:
        return given_aws_data_access_role

    aws_data_access_role = os.getenv(AWS_DATA_ACCESS_ROLE_ENV_KEY, None)

    if aws_data_access_role:
        return aws_data_access_role

    raise RuntimeError(
        "AWS Data Access Role is not set."
        f"Please use environment variable: {AWS_DATA_ACCESS_ROLE_ENV_KEY}"
    )
