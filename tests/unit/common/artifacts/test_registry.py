"""Test Artifact Registry."""
import mock
import pytest

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment, registry_mappers


@mock.patch("phoenix.common.artifacts.registry_mappers.get_default_mappers")
def test_artifact_url_registry__init(m_get_default_mappers, tenant_config):
    """ArtifactURLRegistry init."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    art_url_reg = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt)
    m_get_default_mappers.assert_called_once_with()
    assert art_url_reg.environment_key == environment_key
    assert art_url_reg.tenant_config == tenant_config
    assert art_url_reg.run_dt == run_dt
    assert art_url_reg.mappers == m_get_default_mappers.return_value


@mock.patch("phoenix.common.artifacts.registry_mappers.get_default_mappers")
def test_artifact_url_registry_init_mappers(m_get_default_mappers, tenant_config):
    """ArtifactURLRegistry init."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    mappers: registry_mappers.MapperDict = {"source-posts": "nothing"}  # type: ignore
    art_url_reg = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt, mappers)
    m_get_default_mappers.assert_not_called()
    assert art_url_reg.environment_key == environment_key
    assert art_url_reg.tenant_config == tenant_config
    assert art_url_reg.run_dt == run_dt
    assert art_url_reg.mappers == mappers


@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry._build_url_config")
def test_artifact_url_registry(m_build_url_config, tenant_config):
    """Test ArtifactURLRegistry."""
    artifact_key: registry_mappers.ArtifactKey = "source-posts"
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    url_config = {"RAN": "ran"}
    m_source_mapper = mock.MagicMock(registry_mappers.ArtifactURLMapper)
    m_base_mapper = mock.MagicMock(registry_mappers.ArtifactURLMapper)
    mappers: registry_mappers.MapperDict = {
        "source-posts": m_source_mapper,
        "base-posts": m_base_mapper,  # type: ignore
    }
    aur = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt, mappers)
    r_url = aur.get_url(artifact_key, url_config)
    m_build_url_config.assert_called_once_with(url_config)
    m_source_mapper.assert_called_once_with(
        artifact_key, m_build_url_config.return_value, environment_key, tenant_config
    )
    m_base_mapper.assert_not_called()

    assert r_url == m_source_mapper.return_value


@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry._build_url_config")
def test_artifact_url_registry_value_error(m_build_url_config, tenant_config):
    """Test ArtifactURLRegistry."""
    artifact_key: registry_mappers.ArtifactKey = "source-posts"
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    url_config = {"RAN": "ran"}
    m_source_mapper = mock.MagicMock(spec=registry_mappers.ArtifactURLMapper)
    m_base_mapper = mock.MagicMock(spec=registry_mappers.ArtifactURLMapper)
    mappers: registry_mappers.MapperDict = {
        "source-": m_source_mapper,  # type: ignore
        "base-": m_base_mapper,  # type: ignore
    }
    aur = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt, mappers)
    with pytest.raises(ValueError):
        aur.get_url(artifact_key, url_config)
        m_build_url_config.assert_called_once_with(url_config)
        m_source_mapper.assert_not_called()


def test_artifact_build_url_config_default(tenant_config):
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran"}
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    aur = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == {
        **url_config,
        **{
            "RUN_DATE": run_dt.to_run_date_str(),
            "RUN_DATETIME": run_dt.to_file_safe_str(),
        },
    }


def test_artifact_build_url_config_non_default(tenant_config):
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran", "RUN_DATE": "RUN_DATE"}
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    aur = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == {
        **url_config,
        **{"RUN_DATETIME": run_dt.to_file_safe_str()},
    }


def test_artifact_build_url_config_non_default_all(tenant_config):
    """Test ArtifactURLRegistry._build_url_config."""
    url_config = {"RAN": "ran", "RUN_DATE": "RUN_DATE", "RUN_DATETIME": "RUN_DATETIME"}
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    aur = registry.ArtifactURLRegistry(environment_key, tenant_config, run_dt)
    r_url_config = aur._build_url_config(url_config)
    assert r_url_config == url_config
