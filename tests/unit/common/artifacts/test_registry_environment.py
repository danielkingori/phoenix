"""Test Registry Environments."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_prefrix_error(m_get_local):
    """Test default_url_prefix for default."""
    with pytest.raises(Exception) as excinfo:
        registry_environment.default_url_prefix("key", {"a": "b"}, "non_existant")

    m_get_local.assert_not_called()
    assert "non_existant" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_prefrix_default(m_get_local):
    """Test default_url_prefix for default."""
    result = registry_environment.default_url_prefix("key", {"a": "b"})
    m_get_local.assert_called_once_with()
    assert result == str(m_get_local.return_value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production(m_get_local):
    """Test default_url_prefix for production."""
    expected_result = "s3://bucket/"
    with mock.patch.dict(
        os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: expected_result}
    ):
        result = registry_environment.default_url_prefix("key", {"a": "b"}, "production")
        m_get_local.assert_not_called()
        assert result == expected_result


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_error(m_get_local):
    """Test default_url_prefix for production if env not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(Exception) as excinfo:
            registry_environment.default_url_prefix("key", {"a": "b"}, "production")

    m_get_local.assert_not_called()
    assert registry_environment.PRODUCTION_ENV_VAR_KEY in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_set(m_get_local):
    """Test default_url_prefix for set in the argument."""
    url = "s3://bucket/"
    result = registry_environment.default_url_prefix("key", {"a": "b"}, url)
    m_get_local.assert_not_called()
    assert result == url


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_schema_error(m_get_local):
    """Test default_url_prefix for set in the argument not valid."""
    url = "not://bucket/"
    with pytest.raises(ValueError) as excinfo:
        registry_environment.default_url_prefix("key", {"a": "b"}, url)

    m_get_local.assert_not_called()
    assert url in str(excinfo.value)
    assert "schemas" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_default_url_production_directory_error(m_get_local):
    """Test default_url_prefix for set in the argument not valid."""
    url = "s3://non_directory_bucket"
    with pytest.raises(ValueError) as excinfo:
        registry_environment.default_url_prefix("key", {"a": "b"}, url)

    m_get_local.assert_not_called()
    assert url in str(excinfo.value)
    assert "directory" in str(excinfo.value)


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_prefix_default(m_get_local):
    """Test dashboard_url_prefix for default."""
    result = registry_environment.dashboard_url_prefix("key", {"a": "b"})
    m_get_local.assert_not_called()
    assert result is None


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_production(m_get_local):
    """Test dashboard_url_prefix for production."""
    expected_result = "s3://bucket/"
    with mock.patch.dict(
        os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: expected_result}
    ):
        result = registry_environment.dashboard_url_prefix("key", {"a": "b"}, "production")
        m_get_local.assert_not_called()
        assert result == expected_result


@mock.patch("phoenix.common.artifacts.urls.get_local")
def test_dashboard_url_production_not_set(m_get_local):
    """Test dashboard_url_prefix for production if env not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        result = registry_environment.dashboard_url_prefix("key", {"a": "b"}, "production")
        m_get_local.assert_not_called()
        assert result is None
