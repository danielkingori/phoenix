"""Videos from channel ids run params."""
import datetime

import mock
import pytest
from freezegun import freeze_time

from phoenix.scrape.youtube.run_params import search_videos_from_channel_ids


def test_get_urls():
    """Test get urls."""
    general_run_params = mock.Mock()
    art_url_reg = general_run_params.art_url_reg
    result = search_videos_from_channel_ids._get_urls(general_run_params)
    config = general_run_params.run_dt.to_url_config.return_value
    calls = [
        mock.call("static-youtube_channels", config),
        mock.call("source-youtube_search_videos_from_channel_ids", config),
        mock.call("base-grouped_by_youtube_search_videos", config),
    ]
    art_url_reg.get_url.assert_has_calls(calls)
    assert isinstance(
        result, search_videos_from_channel_ids.ScrapeYouTubeSearchVideosFromChannelIdsURLs
    )
    assert result.config == config
    assert result.static_youtube_channels == art_url_reg.get_url.return_value
    assert result.source_youtube_search_videos_from_channel_ids == art_url_reg.get_url.return_value
    assert result.base_youtube_search_videos == art_url_reg.get_url.return_value


def test_get_urls_given_static_youtube_channels():
    """Test get urls."""
    general_run_params = mock.Mock()
    art_url_reg = general_run_params.art_url_reg
    static_youtube_channels = "static_youtube_channels"
    result = search_videos_from_channel_ids._get_urls(general_run_params, static_youtube_channels)
    config = general_run_params.run_dt.to_url_config.return_value
    calls = [
        mock.call("source-youtube_search_videos_from_channel_ids", config),
        mock.call("base-grouped_by_youtube_search_videos", config),
    ]
    art_url_reg.get_url.assert_has_calls(calls)
    assert isinstance(
        result, search_videos_from_channel_ids.ScrapeYouTubeSearchVideosFromChannelIdsURLs
    )
    assert result.config == config
    assert result.static_youtube_channels == static_youtube_channels
    assert result.source_youtube_search_videos_from_channel_ids == art_url_reg.get_url.return_value
    assert result.base_youtube_search_videos == art_url_reg.get_url.return_value


@mock.patch("phoenix.scrape.youtube.run_params.search_videos_from_channel_ids.get_published_after")
@mock.patch("phoenix.scrape.youtube.run_params.search_videos_from_channel_ids._get_urls")
@mock.patch("phoenix.common.run_params.general.create")
def test_create(m_general_create, m_get_urls, m_published_after):
    """Test create."""
    artifact_env = "artifact_env"
    tenant_id = "tenant_id"
    run_datetime_str = "run_datetime_str"
    scrape_since_days = 3
    static_youtube_channels = "static_youtube_channels"
    result = search_videos_from_channel_ids.create(
        artifact_env, tenant_id, run_datetime_str, scrape_since_days, static_youtube_channels
    )
    m_general_create.assert_called_once_with(artifact_env, tenant_id, run_datetime_str)
    m_get_urls.assert_called_once_with(m_general_create.return_value, static_youtube_channels)
    m_published_after.assert_called_once_with(scrape_since_days)
    assert isinstance(
        result, search_videos_from_channel_ids.ScrapeYouTubeSearchVideosFromChannelIds
    )
    assert result.general == m_general_create.return_value
    assert result.urls == m_get_urls.return_value
    assert result.scrape_since_days == scrape_since_days
    assert result.published_after == m_published_after.return_value


@mock.patch("phoenix.scrape.youtube.run_params.search_videos_from_channel_ids.get_published_after")
@mock.patch("phoenix.scrape.youtube.run_params.search_videos_from_channel_ids._get_urls")
@mock.patch("phoenix.common.run_params.general.create")
def test_create_default(m_general_create, m_get_urls, m_published_after):
    """Test create default."""
    artifact_env = "artifact_env"
    tenant_id = "tenant_id"
    run_datetime_str = "run_datetime_str"
    result = search_videos_from_channel_ids.create(artifact_env, tenant_id, run_datetime_str)
    m_general_create.assert_called_once_with(artifact_env, tenant_id, run_datetime_str)
    m_get_urls.assert_called_once_with(m_general_create.return_value, None)
    m_published_after.assert_called_once_with(None)
    assert isinstance(
        result, search_videos_from_channel_ids.ScrapeYouTubeSearchVideosFromChannelIds
    )
    assert result.general == m_general_create.return_value
    assert result.urls == m_get_urls.return_value
    assert result.scrape_since_days is None
    assert result.published_after == m_published_after.return_value


@freeze_time("2000-01-10 T01:02:03.000004Z")
@pytest.mark.parametrize(
    "scrape_since_days, expected_datetime",
    [
        (None, datetime.datetime(2000, 1, 7, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)),
        (3, datetime.datetime(2000, 1, 7, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)),
        (10, datetime.datetime(1999, 12, 31, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)),
    ],
)
def test_published_after(scrape_since_days, expected_datetime):
    """Test get_published_after."""
    assert expected_datetime == search_videos_from_channel_ids.get_published_after(
        scrape_since_days
    )
