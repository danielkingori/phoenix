"""Search videos from channel ids run params."""
from typing import Any, Dict, Optional

import dataclasses
import datetime

from phoenix.common import artifacts
from phoenix.common.run_params import base, general


DEFAULT_SCRAPE_SINCE_DAYS = 3


@dataclasses.dataclass
class ScrapeYouTubeSearchVideosFromChannelIdsURLs(base.RunParams):
    """URLs for the scraping of youtube videos from channels."""

    config: Dict[str, Any]
    static_youtube_channels: str
    source_youtube_search_videos_from_channel_ids: str
    base_youtube_search_videos: str


@dataclasses.dataclass
class ScrapeYouTubeSearchVideosFromChannelIds(base.RunParams):
    """Scrape videos from channels run params."""

    urls: ScrapeYouTubeSearchVideosFromChannelIdsURLs
    general: general.GeneralRunParams
    scrape_since_days: Optional[int]
    published_after: datetime.datetime


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
    scrape_since_days: Optional[int] = None,
    static_youtube_channels: Optional[str] = None,
):
    """Create for the ScrapeYouTubeSearchVideosFromChannelIds."""
    general_run_params = general.create(artifact_env, tenant_id, run_datetime_str)

    urls = _get_urls(
        general_run_params,
        static_youtube_channels,
    )

    return ScrapeYouTubeSearchVideosFromChannelIds(
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
    general_run_params: general.GeneralRunParams, static_youtube_channels: Optional[str] = None
) -> ScrapeYouTubeSearchVideosFromChannelIdsURLs:
    """Get the URLs."""
    config = general_run_params.run_dt.to_url_config()
    if not static_youtube_channels:
        static_youtube_channels = general_run_params.art_url_reg.get_url(
            "static-youtube_channels", config
        )
    return ScrapeYouTubeSearchVideosFromChannelIdsURLs(
        config=config,
        static_youtube_channels=static_youtube_channels,
        source_youtube_search_videos_from_channel_ids=general_run_params.art_url_reg.get_url(
            "source-youtube_search_videos_from_channel_ids", config
        ),
        base_youtube_search_videos=general_run_params.art_url_reg.get_url(
            "base-grouped_by_youtube_search_videos", config
        ),
    )
