"""Test Artifact Registry returns correct URLs."""
import pytest
from freezegun import freeze_time

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment


@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url",
    [
        (
            "source-posts",
            {},
            ("source_runs/2000-01-01/" "source-posts-20000101T010101.000001Z.json"),
        ),
        (
            "source-facebook_comments",
            {},
            ("source_runs/2000-01-01/" "source-facebook_comments-20000101T010101.000001Z.json"),
        ),
    ],
)
def test_base_grouped_by(artifact_key, url_config, expected_url):
    """Test base grouped by."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    art_url_reg = registry.ArtifactURLRegistry(run_dt, environment_key)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
