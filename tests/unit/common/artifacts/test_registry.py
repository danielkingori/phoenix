"""Test Artifact Registry."""
import datetime

import mock
import pytest

from phoenix.common.artifacts import registry


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_source_url(m_get_local):
    """Test source urls."""
    artifact_key = "source-posts"
    url_config = {"RUN_DATE": "RUN_DATE", "RUN_ISO_TIMESTAMP": "RUN_ISO_TIMESTAMP"}
    environment_key = "local"
    local_path = "file:/local_path/"
    m_get_local.return_value = local_path

    r_url = registry.source_url(artifact_key, url_config, environment_key)

    assert r_url == f"{local_path}RUN_DATE/source_runs/source-posts-RUN_ISO_TIMESTAMP.json"


@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry._build_url_config")
@mock.patch("phoenix.common.artifacts.registry.source_url")
def test_artifact_url_registry(m_source_url, m_build_url_config):
    """Test ArtifactURLRegistry."""
    artifact_key = "source-posts"
    run_datetime = datetime.datetime.now()
    environment_key = "local"
    url_config = {"RAN": "ran"}
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    aur.mappers["source-"] = m_source_url
    r_url = aur.get_url(artifact_key, url_config)
    m_build_url_config.assert_called_once_with(url_config)
    m_source_url.assert_called_once_with(
        artifact_key, m_build_url_config.return_value, environment_key
    )

    assert r_url == m_source_url.return_value


@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry._build_url_config")
@mock.patch("phoenix.common.artifacts.registry.source_url")
def test_artifact_url_registry_value_error(m_source_url, m_build_url_config):
    """Test ArtifactURLRegistry."""
    artifact_key = "notsource-posts"
    run_datetime = datetime.datetime.now()
    environment_key = "local"
    url_config = {"RAN": "ran"}
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    aur.mappers["source-"] = m_source_url
    with pytest.raises(ValueError):
        aur.get_url(artifact_key, url_config)
        m_build_url_config.assert_called_once_with(url_config)
        m_source_url.assert_not_called()


def test_artiface_build_url_config_default():
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran"}
    run_datetime = datetime.datetime.now()
    environment_key = "local"
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == {
        **url_config,
        **{
            "RUN_DATE": run_datetime.strftime("%Y-%m-%d"),
            "RUN_ISO_TIMESTAMP": run_datetime.isoformat(),
        },
    }


def test_artiface_build_url_config_non_default():
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran", "RUN_DATE": "RUN_DATE"}
    run_datetime = datetime.datetime.now()
    environment_key = "local"
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == {
        **url_config,
        **{
            "RUN_ISO_TIMESTAMP": run_datetime.isoformat(),
        },
    }


def test_artiface_build_url_config_non_default_all():
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran", "RUN_DATE": "RUN_DATE", "RUN_ISO_TIMESTAMP": "RUN_ISO_TIMESTAMP"}
    run_datetime = datetime.datetime.now()
    environment_key = "local"
    aur = registry.ArtifactURLRegistry(run_datetime, environment_key)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == url_config
