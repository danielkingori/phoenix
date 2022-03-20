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
    exclude_accounts: Union[List[str], None]
    has_topics: bool
    custom_prefix: Union[str, None]
    head: int
    after_timestamp: Union[datetime.datetime, None]
    before_timestamp: Union[datetime.datetime, None]


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
    after_timestamp: Optional[Union[str, datetime.datetime]],
    before_timestamp: Optional[Union[str, datetime.datetime]],
    exclude_accounts: Optional[Union[List[str], str]],
) -> ExportManualScrapingRunParams:
    """Create the ExportManualScrapingRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(general_run_params, object_type, year_filter, month_filter, custom_prefix)
    include_accounts = normalise_account_parameter(include_accounts)
    exclude_accounts = normalise_account_parameter(exclude_accounts)
    normalised_has_topics = True
    if has_topics is not None:
        normalised_has_topics = utils.normalise_bool(has_topics)

    normalised_head = utils.normalise_int(head)
    if not normalised_head:
        normalised_head = DEFAULT_HEAD

    normalised_after_timestamp = normalise_datetime(after_timestamp)
    normalised_before_timestamp = normalise_datetime(before_timestamp)

    return ExportManualScrapingRunParams(
        general=general_run_params,
        urls=urls,
        include_accounts=include_accounts,
        has_topics=normalised_has_topics,
        custom_prefix=custom_prefix,
        head=normalised_head,
        after_timestamp=normalised_after_timestamp,
        before_timestamp=normalised_before_timestamp,
        exclude_accounts=exclude_accounts,
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


def normalise_datetime(
    dt: Optional[Union[str, datetime.datetime]],
) -> Union[datetime.datetime, None]:
    """Normalise a datetime."""
    if not dt:
        return None

    if type(dt) is str:
        return _datetime_from_string(dt)

    if type(dt) is datetime.datetime:
        return dt

    if type(dt) is datetime.date:
        dt = datetime.datetime.combine(dt, datetime.datetime.min.time())
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

    raise ValueError(f"Unable to convert to datetime: {dt} with type: {type(dt)}")


def _datetime_from_string(dt_str: str) -> datetime.datetime:
    dt = datetime.datetime.fromisoformat(dt_str)
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt
