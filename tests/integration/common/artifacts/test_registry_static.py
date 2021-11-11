"""Test Artifact Registry returns correct URLs."""
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common.artifacts import registry_environment
from tests.integration.common.artifacts import conftest


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: "s3://bucket/"})
@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url, environment",
    [
        (
            "static-twitter_users",
            {},
            "/local_artifacts/config/twitter_query_users.csv",
            "local",
        ),
        (
            "static-twitter_users",
            {},
            "s3://bucket/config/twitter_query_users.csv",
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
    art_url_reg = conftest.create_test_art_url_reg(environment)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
