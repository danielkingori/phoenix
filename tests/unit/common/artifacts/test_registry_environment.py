"""Test Registry Environments."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_prefrix_error(m_get_local):
    """Test default_url_prefix for default."""
    with pytest.raises(Exception) as excinfo:
        registry_environment.default_url_prefix("key", {"a": "b"}, "non_existant", "test_tenant")

    m_get_local.assert_not_called()
    assert "non_existant" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_prefrix_local(m_get_local):
    """Test default_url_prefix for local."""
    tenant_id = "test_tenant"
    result = registry_environment.default_url_prefix("key", {"a": "b"}, "local", tenant_id)
    m_get_local.assert_called_once_with()
    assert result == f"{str(m_get_local.return_value)}{tenant_id}/"


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production(m_get_local):
    """Test default_url_prefix for production."""
    production_url = "s3://bucket/"
    tenant_id = "test_tenant"
    with mock.patch.dict(
        os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: production_url}
    ):
        result = registry_environment.default_url_prefix(
            "key", {"a": "b"}, "production", tenant_id
        )
        m_get_local.assert_not_called()
        assert result == f"{production_url}{tenant_id}/"


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_error(m_get_local):
    """Test default_url_prefix for production if env not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError) as excinfo:
            registry_environment.default_url_prefix("key", {"a": "b"}, "production", "test_tenant")

    m_get_local.assert_not_called()
    assert registry_environment.PRODUCTION_ENV_VAR_KEY in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_error_not_directory(m_get_local):
    """Test default_url_prefix for production error as not directory."""
    production_url = "s3://bucket"
    tenant_id = "test_tenant"
    with mock.patch.dict(
        os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: production_url}
    ):
        with pytest.raises(ValueError) as excinfo:
            registry_environment.default_url_prefix("key", {"a": "b"}, "production", tenant_id)
    m_get_local.assert_not_called()
    assert registry_environment.PRODUCTION_ENV_VAR_KEY in str(excinfo.value)
    assert "/" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_error_schema(m_get_local):
    """Test default_url_prefix for production error with schema."""
    production_url = "not_supported://bucket"
    tenant_id = "test_tenant"
    with mock.patch.dict(
        os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: production_url}
    ):
        with pytest.raises(ValueError) as excinfo:
            registry_environment.default_url_prefix("key", {"a": "b"}, "production", tenant_id)
    m_get_local.assert_not_called()
    assert registry_environment.PRODUCTION_ENV_VAR_KEY in str(excinfo.value)
    assert "not_supported" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_set(m_get_local):
    """Test default_url_prefix for set in the argument."""
    url = "s3://bucket/"
    tenant_id = "test_tenant"
    result = registry_environment.default_url_prefix("key", {"a": "b"}, url, tenant_id)
    m_get_local.assert_not_called()
    assert result == f"{url}{tenant_id}/"


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_schema_error(m_get_local):
    """Test default_url_prefix for set in the argument not valid."""
    url = "not://bucket/"
    with pytest.raises(ValueError) as excinfo:
        registry_environment.default_url_prefix("key", {"a": "b"}, url, "test_tenant")

    m_get_local.assert_not_called()
    assert url in str(excinfo.value)
    assert "schemas" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_directory_error(m_get_local):
    """Test default_url_prefix for set in the argument not valid."""
    url = "s3://non_directory_bucket"
    with pytest.raises(ValueError) as excinfo:
        registry_environment.default_url_prefix("key", {"a": "b"}, url, "test_tenant")

    m_get_local.assert_not_called()
    assert url in str(excinfo.value)
    assert "directory" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_prefix_local(m_get_local):
    """Test dashboard_url_prefix for local."""
    result = registry_environment.dashboard_url_prefix("key", {"a": "b"}, "local", "test_tenant")
    m_get_local.assert_not_called()
    assert result is None


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_production(m_get_local):
    """Test dashboard_url_prefix for production."""
    url = "s3://bucket/"
    tenant_id = "test_tenant"
    with mock.patch.dict(os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: url}):
        result = registry_environment.dashboard_url_prefix(
            "key", {"a": "b"}, "production", tenant_id
        )
        m_get_local.assert_not_called()
        assert result == "s3://bucket/test_tenant/"


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_production_error_not_directory(m_get_local):
    """Test dashboard_url_prefix for production error."""
    url = "s3://bucket"
    tenant_id = "test_tenant"
    with mock.patch.dict(os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: url}):
        with pytest.raises(ValueError) as excinfo:
            registry_environment.dashboard_url_prefix("key", {"a": "b"}, "production", tenant_id)
    m_get_local.assert_not_called()
    assert registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY in str(excinfo.value)
    assert "/" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_production_error_schema(m_get_local):
    """Test dashboard_url_prefix for production error."""
    url = "not_schema://bucket/"
    tenant_id = "test_tenant"
    with mock.patch.dict(os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: url}):
        with pytest.raises(ValueError) as excinfo:
            registry_environment.dashboard_url_prefix("key", {"a": "b"}, "production", tenant_id)
    m_get_local.assert_not_called()
    assert registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY in str(excinfo.value)
    assert "not_schema" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_production_not_set(m_get_local):
    """Test dashboard_url_prefix for production if env not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        result = registry_environment.dashboard_url_prefix(
            "key", {"a": "b"}, "production", "test_tenant"
        )
        m_get_local.assert_not_called()
        assert result is None
