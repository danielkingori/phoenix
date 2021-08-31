"""Test Artifact Registry."""
import datetime

import mock
import pytest

from phoenix.common.artifacts import registry, registry_environment, registry_mappers


@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry._build_url_config")
def test_artifact_url_registry(m_build_url_config):
    """Test ArtifactURLRegistry."""
    artifact_key: registry_mappers.ArtifactKey = "source-posts"
    run_datetime = datetime.datetime.now()
    environment_key: registry_environment.Environments = "local"
    url_config = {"RAN": "ran"}
    m_source_mapper = mock.MagicMock(registry_mappers.ArtifactURLMapper)
    m_base_mapper = mock.MagicMock(registry_mappers.ArtifactURLMapper)
    mappers = {
        "source-posts": m_source_mapper,
        "base-posts": m_base_mapper,
    }
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key, mappers)  # type: ignore
    r_url = aur.get_url(artifact_key, url_config)
    m_build_url_config.assert_called_once_with(url_config)
    m_source_mapper.assert_called_once_with(
        artifact_key, m_build_url_config.return_value, environment_key
    )
    m_base_mapper.assert_not_called()

    assert r_url == m_source_mapper.return_value


@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry._build_url_config")
def test_artifact_url_registry_value_error(m_build_url_config):
    """Test ArtifactURLRegistry."""
    artifact_key: registry_mappers.ArtifactKey = "source-posts"
    run_datetime = datetime.datetime.now()
    environment_key: registry_environment.Environments = "local"
    url_config = {"RAN": "ran"}
    m_source_mapper = mock.MagicMock(registry_mappers.ArtifactURLMapper)
    m_base_mapper = mock.MagicMock(registry_mappers.ArtifactURLMapper)
    mappers = {
        "source-": m_source_mapper,
        "base-": m_base_mapper,
    }
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key, mappers)  # type: ignore
    with pytest.raises(ValueError):
        aur.get_url(artifact_key, url_config)
        m_build_url_config.assert_called_once_with(url_config)
        m_source_mapper.assert_not_called()


def test_artifact_build_url_config_default():
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran"}
    run_datetime = datetime.datetime.now()
    environment_key: registry_environment.Environments = "local"
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == {
        **url_config,
        **{
            "RUN_DATE": run_datetime.strftime("%Y-%m-%d"),
            "RUN_ISO_TIMESTAMP": run_datetime.isoformat(),
        },
    }


def test_artifact_build_url_config_non_default():
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran", "RUN_DATE": "RUN_DATE"}
    run_datetime = datetime.datetime.now()
    environment_key: registry_environment.Environments = "local"
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == {
        **url_config,
        **{
            "RUN_ISO_TIMESTAMP": run_datetime.isoformat(),
        },
    }


def test_artifact_build_url_config_non_default_all():
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran", "RUN_DATE": "RUN_DATE", "RUN_ISO_TIMESTAMP": "RUN_ISO_TIMESTAMP"}
    run_datetime = datetime.datetime.now()
    environment_key: registry_environment.Environments = "local"
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == url_config
