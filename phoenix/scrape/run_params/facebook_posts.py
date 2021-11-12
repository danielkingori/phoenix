"""Facebook posts run params."""
from typing import List, Optional

import dataclasses
import datetime

from phoenix.common import artifacts
from phoenix.common.run_params import general
from phoenix.scrape import crowdtangle


@dataclasses.dataclass
class ScrapeFacebookPostsRunParams(general.GeneralRunParams):
    """Scrape facebook posts run params."""

    scrape_list_ids: List[str]
    scrape_list_id: str
    scrape_start_date: datetime.datetime
    scrape_end_date: datetime.datetime
    artifact_source_fb_posts_url: str
    since_days: Optional[int]


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    scrape_list_id: str,
    run_datetime_str: Optional[str] = None,
    artifact_source_fb_posts_url: Optional[str] = None,
    since_days: Optional[int] = None,
    scrape_start_date: Optional[datetime.datetime] = None,
    scrape_end_date: Optional[datetime.datetime] = None,
):
    """Create for the ScrapeFacebookPostsRunParams."""
    art_url_reg, tenant_config, run_dt = general.create_base_objects(
        artifact_env, tenant_id, run_datetime_str
    )
    if artifact_source_fb_posts_url is None:
        artifact_source_fb_posts_url = art_url_reg.get_url("source-posts")

    if since_days:
        since_days = int(since_days)
        scrape_end_date = run_dt.dt
        scrape_start_date = run_dt.dt - datetime.timedelta(days=int(since_days))

    if scrape_start_date is None:
        scrape_start_date = run_dt.dt - datetime.timedelta(days=3)

    if scrape_end_date is None:
        scrape_end_date = run_dt.dt

    # This will get the list ids from the parameter of an env variable
    scrape_list_ids = crowdtangle.process_scrape_list_id(scrape_list_id)
    return ScrapeFacebookPostsRunParams(
        scrape_list_ids=scrape_list_ids,
        scrape_list_id=scrape_list_id,
        scrape_start_date=scrape_start_date,
        scrape_end_date=scrape_end_date,
        artifact_source_fb_posts_url=artifact_source_fb_posts_url,
        since_days=since_days,
        # GenearlRunParams
        run_dt=run_dt,
        tenant_config=tenant_config,
        art_url_reg=art_url_reg,
    )
