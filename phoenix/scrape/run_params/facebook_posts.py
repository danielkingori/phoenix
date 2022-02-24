"""RunParams facebook posts."""
from typing import Any, Dict, List, Optional, Tuple, Union

import dataclasses
import datetime

from phoenix.common.run_params import base, general
from phoenix.scrape import crowdtangle


DATE_FORMAT = "%Y-%m-%d"
DEFAULT_SCRAPE_SINCE_DAYS = 3


@dataclasses.dataclass
class FacebookPostsScrapeRunParamsURLs(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    source: str


@dataclasses.dataclass
class FacebookPostsScrapeRunParams(base.RunParams):
    """Finalise accounts run params."""

    urls: FacebookPostsScrapeRunParamsURLs
    general: general.GeneralRunParams
    scrape_since_days: Optional[int]
    scrape_end_date: datetime.datetime
    scrape_start_date: datetime.datetime
    crowdtangle_list_ids: List[str]


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    scrape_since_days: Optional[Union[str, int]] = None,
    scrape_start_date: Optional[Union[datetime.datetime, str]] = None,
    scrape_end_date: Optional[Union[datetime.datetime, str]] = None,
    crowdtangle_list_ids: Optional[Union[List[str], str]] = None,
) -> FacebookPostsScrapeRunParams:
    """Create FacebookPostsScrapeRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)

    art_url_reg = general_run_params.art_url_reg

    crowdtangle_list_ids = get_crowdtangle_list_ids(
        crowdtangle_list_ids, general_run_params.tenant_config
    )

    url_config: Dict[str, Any] = {}
    urls = FacebookPostsScrapeRunParamsURLs(
        config=url_config,
        source=art_url_reg.get_url("source-posts", url_config),
    )
    scrape_since_days, scrape_start_date, scrape_end_date = get_scraping_range(
        general_run_params, scrape_since_days, scrape_start_date, scrape_end_date
    )
    return FacebookPostsScrapeRunParams(
        general=general_run_params,
        urls=urls,
        scrape_since_days=scrape_since_days,
        scrape_start_date=scrape_start_date,
        scrape_end_date=scrape_end_date,
        crowdtangle_list_ids=crowdtangle_list_ids,
    )


def parse_scrape_date(date_str: str) -> datetime.datetime:
    """Parse the string into a date."""
    dt = datetime.datetime.strptime(date_str, DATE_FORMAT)
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def get_crowdtangle_list_ids(crowdtangle_list_ids, tenant_config) -> List[str]:
    """Get the crowdtangle_list_ids."""
    if isinstance(crowdtangle_list_ids, str):
        crowdtangle_list_ids = crowdtangle.process_scrape_list_id(crowdtangle_list_ids)
    elif isinstance(crowdtangle_list_ids, list):
        crowdtangle_list_ids = crowdtangle_list_ids
    else:
        crowdtangle_list_ids = crowdtangle.process_scrape_list_id(
            tenant_config.crowdtangle_scrape_list_id
        )

    if not crowdtangle_list_ids:
        raise ValueError("CrowdTangle List Ids to scrape is needed")

    return crowdtangle_list_ids


def get_scraping_range(
    general_run_params: general.GeneralRunParams,
    scrape_since_days: Optional[Union[str, int]] = None,
    scrape_start_date: Optional[Union[datetime.datetime, str]] = None,
    scrape_end_date: Optional[Union[datetime.datetime, str]] = None,
) -> Tuple[Union[int, None], datetime.datetime, datetime.datetime]:
    """Get the scraping range."""
    if scrape_since_days and scrape_start_date:
        raise ValueError("Cannot set scrape_since_days and scrape_start_date.")

    if scrape_since_days or scrape_since_days == 0:
        scrape_since_days = int(scrape_since_days)
    else:
        scrape_since_days = DEFAULT_SCRAPE_SINCE_DAYS

    if isinstance(scrape_end_date, str):
        scrape_end_date = parse_scrape_date(scrape_end_date)

    if not scrape_end_date:
        scrape_end_date = general_run_params.run_dt.dt

    if isinstance(scrape_start_date, str):
        scrape_start_date = parse_scrape_date(scrape_start_date)

    if scrape_start_date:
        scrape_since_days = None
    else:
        scrape_start_date = scrape_end_date - datetime.timedelta(days=int(scrape_since_days))

    return (scrape_since_days, scrape_start_date, scrape_end_date)
