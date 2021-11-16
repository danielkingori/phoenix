"""Video from channel ids run params."""
from typing import Any, Dict, Optional

import dataclasses
import datetime

from phoenix.common import artifacts
from phoenix.common.run_params import base, general


DEFAULT_SCRAPE_SINCE_DAYS = 3


@dataclasses.dataclass
class ScrapeYouTubeVideosFromChannelIdsURLs(base.RunParams):
    """URLs for the scraping of youtube videos from channels."""

    config: Dict[str, Any]
    static_youtube_channels: str
    source_youtube_videos_from_channel_ids: str
    base_youtube_searches: str


@dataclasses.dataclass
class ScrapeYouTubeVideosFromChannelIds(base.RunParams):
    """Scrape videos from channels run params."""

    urls: ScrapeYouTubeVideosFromChannelIdsURLs
    general: general.GeneralRunParams
    scrape_since_days: Optional[int]
    published_after: datetime.datetime


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
    scrape_since_days: Optional[int] = None,
):
    """Create for the ScrapeYouTubeVideosFromChannelIds."""
    general_run_params = general.create(artifact_env, tenant_id, run_datetime_str)

    urls = _get_urls(
        general_run_params,
    )

    return ScrapeYouTubeVideosFromChannelIds(
        urls=urls,
        general=general_run_params,
        scrape_since_days=scrape_since_days,
        published_after=get_published_after(scrape_since_days),
    )


def get_published_after(scrape_since_days: Optional[int]):
    """Get the published_after required for the videos get."""
    if not scrape_since_days:
        scrape_since_days = DEFAULT_SCRAPE_SINCE_DAYS
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=scrape_since_days
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
) -> ScrapeYouTubeVideosFromChannelIdsURLs:
    """Get the URLs."""
    config = general_run_params.run_dt.to_url_config()
    return ScrapeYouTubeVideosFromChannelIdsURLs(
        config=config,
        static_youtube_channels=general_run_params.art_url_reg.get_url(
            "static-youtube_channels", config
        ),
        source_youtube_videos_from_channel_ids=general_run_params.art_url_reg.get_url(
            "source-youtube_videos_from_channel_ids", config
        ),
        base_youtube_searches=general_run_params.art_url_reg.get_url(
            "base-grouped_by_youtube_searches", config
        ),
    )
