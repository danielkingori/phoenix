"""Channel from channel ids run params."""
from typing import Any, Dict, Optional

import dataclasses

from phoenix.common import artifacts
from phoenix.common.run_params import base, general


@dataclasses.dataclass
class ScrapeYouTubeChannelsFromChannelsURLS(base.RunParams):
    """URLS."""

    config: Dict[str, Any]
    static_youtube_channels: str
    source_youtube_channels_from_channel_ids: str
    base_youtube_channels: str


@dataclasses.dataclass
class ScrapeYouTubeChannelsFromChannelIds(base.RunParams):
    """Scrape channels from channels run params."""

    urls: ScrapeYouTubeChannelsFromChannelsURLS
    general: general.GeneralRunParams


def create(
    artifact_env: artifacts.registry_environment.Environments,
    tenant_id: str,
    run_datetime_str: Optional[str] = None,
):
    """Create for the ScrapeYouTubeChannelsFromChannels."""
    general_run_params = general.create(artifact_env, tenant_id, run_datetime_str)

    urls = _get_urls(
        general_run_params,
    )

    return ScrapeYouTubeChannelsFromChannelIds(urls=urls, general=general_run_params)


def _get_urls(
    general_run_params: general.GeneralRunParams,
) -> ScrapeYouTubeChannelsFromChannelsURLS:
    """Get the URLs."""
    config = general_run_params.run_dt.to_url_config()
    return ScrapeYouTubeChannelsFromChannelsURLS(
        config=config,
        static_youtube_channels=general_run_params.art_url_reg.get_url(
            "static-youtube_channels", config
        ),
        source_youtube_channels_from_channel_ids=general_run_params.art_url_reg.get_url(
            "source-youtube_channels_from_channel_ids", config
        ),
        base_youtube_channels=general_run_params.art_url_reg.get_url(
            "base-grouped_by_youtube_channels", config
        ),
    )
