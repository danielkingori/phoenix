"""Test Artifact Registry returns correct URLs."""
import pytest
from freezegun import freeze_time

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment


@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url, environment",
    [
        (
            "static-twitter_users",
            {},
            "/phoenix/common/config/twitter_query_users.csv",
            "local",
        ),
        (
            "static-twitter_users",
            {},
            "/phoenix/common/config/twitter_query_users.csv",
            "production",
        ),
        (
            "static-custom_models_tension_classifier_base",
            {},
            "/custom_models/tension_classifier/",
            "local",
        ),
    ],
)
def test_static(artifact_key, url_config, expected_url, environment):
    """Test static urls."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = environment
    art_url_reg = registry.ArtifactURLRegistry(run_dt, environment_key)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
    assert result_url.startswith("file")
