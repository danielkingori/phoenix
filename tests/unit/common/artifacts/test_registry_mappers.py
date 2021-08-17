"""Test Artifact Registy Mappers."""
import mock
import pytest

from phoenix.common.artifacts import registry_environment, registry_mappers


@mock.patch("phoenix.common.artifacts.registry_environment.default_url_prefix")
def test_url_mapper(m_default_url_prefix):
    """Test source urls."""
    artifact_key: registry_mappers.ArifactKey = "source-posts"
    url_config = {"RUN_DATE": "RUN_DATE", "RUN_ISO_TIMESTAMP": "RUN_ISO_TIMESTAMP"}
    format_str = "suffix/{RUN_DATE}/file-{RUN_ISO_TIMESTAMP}.json"
    environment_key: registry_environment.Environments = "local"
    default_url_prefix = "file:/local_path/"
    m_default_url_prefix.return_value = default_url_prefix

    r_url = registry_mappers.url_mapper(format_str, artifact_key, url_config, environment_key)

    assert r_url == f"{default_url_prefix}suffix/RUN_DATE/file-RUN_ISO_TIMESTAMP.json"


@mock.patch("phoenix.common.artifacts.registry_environment.default_url_prefix")
def test_url_mapper_url_config_key_not_found(m_default_url_prefix):
    """Test source urls."""
    artifact_key: registry_mappers.ArifactKey = "source-posts"
    url_config = {"KEY": "Value"}
    format_str = "suffix/{RUN_DATE}/file.json"
    environment_key: registry_environment.Environments = "local"
    default_url_prefix = "file:/local_path/"
    m_default_url_prefix.return_value = default_url_prefix

    with pytest.raises(KeyError) as e:
        registry_mappers.url_mapper(format_str, artifact_key, url_config, environment_key)
        assert "RUN_DATE" in str(e)
