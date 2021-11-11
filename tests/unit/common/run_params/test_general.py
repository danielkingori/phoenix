"""GeneralRunParams."""
import mock

from phoenix.common.run_params import general


@mock.patch("phoenix.common.config.tenant.TenantConfig")
@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry")
@mock.patch("phoenix.common.run_datetime.create_run_datetime_now")
@mock.patch("phoenix.common.run_datetime.from_file_safe_str")
def test_create(m_run_dt_str, m_run_dt_now, m_art_url_reg, m_tenant_config):
    """Test create GeneralRunParams."""
    run_datetime_str = "run_datetime"
    environment_key = "local"
    tenant_id = "tenant_id"
    cur_run_params = general.create(environment_key, tenant_id, run_datetime_str)
    m_run_dt_str.assert_called_once_with(run_datetime_str)
    m_run_dt_now.assert_not_called()
    m_tenant_config.assert_called_once_with(tenant_id)
    m_art_url_reg.assert_called_once_with(
        environment_key, m_tenant_config.return_value, m_run_dt_str.return_value
    )
    assert isinstance(cur_run_params, general.GeneralRunParams)
    assert cur_run_params.art_url_reg == m_art_url_reg.return_value
    assert cur_run_params.tenant_config == m_tenant_config.return_value
    assert cur_run_params.run_dt == m_run_dt_str.return_value


@mock.patch("phoenix.common.config.tenant.TenantConfig")
@mock.patch("phoenix.common.artifacts.registry.ArtifactURLRegistry")
@mock.patch("phoenix.common.run_datetime.create_run_datetime_now")
@mock.patch("phoenix.common.run_datetime.from_file_safe_str")
def test_create_none_run_dt(m_run_dt_str, m_run_dt_now, m_art_url_reg, m_tenant_config):
    """Test create with a none run datetime."""
    environment_key = "local"
    tenant_id = "tenant_id"
    cur_run_params = general.create(environment_key, tenant_id)
    m_run_dt_now.assert_called_once_with()
    m_run_dt_str.assert_not_called()
    m_tenant_config.assert_called_once_with(tenant_id)
    m_art_url_reg.assert_called_once_with(
        environment_key, m_tenant_config.return_value, m_run_dt_now.return_value
    )
    assert isinstance(cur_run_params, general.GeneralRunParams)
    assert cur_run_params.art_url_reg == m_art_url_reg.return_value
    assert cur_run_params.tenant_config == m_tenant_config.return_value
    assert cur_run_params.run_dt == m_run_dt_now.return_value
