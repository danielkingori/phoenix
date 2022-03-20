"""RunParams for the export manual scraping."""
from typing import Any, Dict, List, Optional, Union

import dataclasses
import datetime

from phoenix.common.run_params import base, general, utils


DEFAULT_HEAD = 10


@dataclasses.dataclass
class ExportManualScrapingRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    input_dataset: str
    custom_facebook_posts_to_scrape: str


@dataclasses.dataclass
class ExportManualScrapingRunParams(base.RunParams):
    """RunParams."""

    urls: ExportManualScrapingRunParamsURLs
    general: general.GeneralRunParams
    include_accounts: Union[List[str], None]
    has_topics: bool
    custom_prefix: Union[str, None]
    head: int


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
    include_accounts: Optional[Union[List[str], str]],
    has_topics: Optional[Union[bool, str]],
    custom_prefix: Optional[str],
    head: Optional[Union[str, int]],
) -> ExportManualScrapingRunParams:
    """Create the ExportManualScrapingRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(general_run_params, object_type, year_filter, month_filter, custom_prefix)
    include_accounts = normalise_account_parameter(include_accounts)
    normalised_has_topics = True
    if has_topics is not None:
        normalised_has_topics = utils.normalise_bool(has_topics)

    normalised_head = utils.normalise_int(head)
    if not normalised_head:
        normalised_head = DEFAULT_HEAD

    return ExportManualScrapingRunParams(
        general=general_run_params,
        urls=urls,
        include_accounts=include_accounts,
        has_topics=normalised_has_topics,
        custom_prefix=custom_prefix,
        head=normalised_head,
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
    object_type: str,
    year_filter: int,
    month_filter: int,
    custom_prefix: Optional[str],
) -> ExportManualScrapingRunParamsURLs:
    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
        "CUSTOM_PREFIX": custom_prefix if custom_prefix else "",
    }
    return ExportManualScrapingRunParamsURLs(
        config=url_config,
        input_dataset=art_url_reg.get_url("tagging_runs-facebook_posts_final", url_config),
        custom_facebook_posts_to_scrape=art_url_reg.get_url(
            "tagging_runs-custom_facebook_posts_to_scrape", url_config
        ),
    )


def normalise_account_parameter(
    accounts: Optional[Union[List[str], str]],
) -> Union[List[str], None]:
    """Normalise the account parameter."""
    if not accounts:
        return None

    if type(accounts) is str:
        accounts = accounts.split(",")

    if type(accounts) is not list:
        raise ValueError("Accounts parameter is not a list.")

    return accounts
