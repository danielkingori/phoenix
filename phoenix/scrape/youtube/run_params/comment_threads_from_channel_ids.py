"""Run params for scraping comment threads from channel ids."""
from typing import Any, Dict, Optional

import dataclasses

from phoenix.common import artifacts
from phoenix.common.run_params import base, general


@dataclasses.dataclass
class ScrapeYouTubeCommentThreadsFromChannelsURLs(base.RunParams):
    """Dataclass for storing URLs needed in scrape run.."""

    config: Dict[str, Any]
    static_youtube_channels: str
    source_youtube_comment_threads_from_channel_ids: str
    base_youtube_comment_threads: str


@dataclasses.dataclass
class ScrapeYouTubeVideosFromChannelIds(base.RunParams):
    """Run params for scraping comment threads from channels ids."""

    urls: ScrapeYouTubeCommentThreadsFromChannelsURLs
    general: general.GeneralRunParams


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
):
    """Create run params ScrapeYouTubeVideosFromChannelIds."""
    general_run_params = general.create(artifact_env, tenant_id, run_datetime_str)

    urls = _get_urls(
        general_run_params,
    )

    return ScrapeYouTubeVideosFromChannelIds(
        urls=urls,
        general=general_run_params,
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
) -> ScrapeYouTubeCommentThreadsFromChannelsURLs:
    """Get the URLs for ScrapeYouTubeCommentThreadsFromChannelsURLS."""
    config = general_run_params.run_dt.to_url_config()
    return ScrapeYouTubeCommentThreadsFromChannelsURLs(
        config=config,
        static_youtube_channels=general_run_params.art_url_reg.get_url(
            "static-youtube_channels", config
        ),
        source_youtube_comment_threads_from_channel_ids=general_run_params.art_url_reg.get_url(
            "source-youtube_comment_threads_from_channel_ids", config
        ),
        base_youtube_comment_threads=general_run_params.art_url_reg.get_url(
            "base-grouped_by_youtube_comment_threads", config
        ),
    )
