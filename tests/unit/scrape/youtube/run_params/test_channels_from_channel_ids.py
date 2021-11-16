"""Channels From Channels Run params."""
import mock

from phoenix.scrape.youtube.run_params import channels_from_channel_ids


def test_get_urls():
    """Test get urls."""
    general_run_params = mock.Mock()
    art_url_reg = general_run_params.art_url_reg
    result = channels_from_channel_ids._get_urls(general_run_params)
    config = general_run_params.run_dt.to_url_config.return_value
    calls = [
        mock.call("static-youtube_channels", config),
        mock.call("source-youtube_channels_from_channel_ids", config),
        mock.call("base-grouped_by_youtube_channels", config),
    ]
    art_url_reg.get_url.assert_has_calls(calls)
    assert isinstance(result, channels_from_channel_ids.ScrapeYouTubeChannelsFromChannelsURLS)
    assert result.config == config
    assert result.static_youtube_channels == art_url_reg.get_url.return_value
    assert result.source_youtube_channels_from_channel_ids == art_url_reg.get_url.return_value
    assert result.base_youtube_channels == art_url_reg.get_url.return_value


@mock.patch("phoenix.scrape.youtube.run_params.channels_from_channel_ids._get_urls")
@mock.patch("phoenix.common.run_params.general.create")
def test_create(m_general_create, m_get_urls):
    """Test create."""
    artifact_env = "artifact_env"
    tenant_id = "tenant_id"
    run_datetime_str = "run_datetime_str"
    result = channels_from_channel_ids.create(artifact_env, tenant_id, run_datetime_str)
    m_general_create.assert_called_once_with(artifact_env, tenant_id, run_datetime_str)
    m_get_urls.assert_called_once_with(m_general_create.return_value)
    assert isinstance(result, channels_from_channel_ids.ScrapeYouTubeChannelsFromChannelIds)
    assert result.general == m_general_create.return_value
    assert result.urls == m_get_urls.return_value
