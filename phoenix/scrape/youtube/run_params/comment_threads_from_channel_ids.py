"""Run params for scraping comment threads from channel ids."""
from typing import Any, Dict, Optional, Union

import dataclasses

from phoenix.common import artifacts
from phoenix.common.run_params import base, general, utils


@dataclasses.dataclass
class ScrapeYouTubeCommentThreadsFromChannelsURLs(base.RunParams):
    """Dataclass for storing URLs needed in scrape run.."""

    config: Dict[str, Any]
    static_youtube_channels: str
    source_youtube_comment_threads_from_channel_ids: str
    base_youtube_comment_threads: str


@dataclasses.dataclass
class ScrapeYouTubeCommentThreadsFromChannelIds(base.RunParams):
    """Run params for scraping comment threads from channels ids."""

    urls: ScrapeYouTubeCommentThreadsFromChannelsURLs
    general: general.GeneralRunParams
    max_pages: int


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
    max_pages: Optional[Union[str, int]] = None,
    static_youtube_channels: Optional[str] = None,
) -> ScrapeYouTubeCommentThreadsFromChannelIds:
    """Create run params ScrapeYouTubeCommentThreadsFromChannelIds."""
    general_run_params = general.create(artifact_env, tenant_id, run_datetime_str)

    urls = _get_urls(
        general_run_params,
        static_youtube_channels,
    )
    nor_max_pages = utils.normalise_int(max_pages)
    if not nor_max_pages:
        nor_max_pages = 10

    return ScrapeYouTubeCommentThreadsFromChannelIds(
        urls=urls,
        general=general_run_params,
        max_pages=nor_max_pages,
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
    static_youtube_channels: Optional[str] = None,
) -> ScrapeYouTubeCommentThreadsFromChannelsURLs:
    """Get the URLs for ScrapeYouTubeCommentThreadsFromChannelsURLS."""
    config = general_run_params.run_dt.to_url_config()
    if not static_youtube_channels:
        static_youtube_channels = general_run_params.art_url_reg.get_url(
            "static-youtube_channels", config
        )
    return ScrapeYouTubeCommentThreadsFromChannelsURLs(
        config=config,
        static_youtube_channels=static_youtube_channels,
        source_youtube_comment_threads_from_channel_ids=general_run_params.art_url_reg.get_url(
            "source-youtube_comment_threads_from_channel_ids", config
        ),
        base_youtube_comment_threads=general_run_params.art_url_reg.get_url(
            "base-grouped_by_youtube_comment_threads", config
        ),
    )
