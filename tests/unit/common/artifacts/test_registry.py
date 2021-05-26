"""Test Artifact Registry."""
import mock

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
