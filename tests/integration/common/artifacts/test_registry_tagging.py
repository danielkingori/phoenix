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
            "tagging_runs-facebook_posts_input",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "base/grouped_by_year_month/facebook_posts/year_filter=2021/month_filter=1/",
        ),
        (
            "tagging_runs-facebook_posts_pulled",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "tagging_runs/year_filter=2021/month_filter=1/"
                "facebook_posts/facebook_posts_pulled.parquet"
            ),
        ),
        (
            "tagging_runs-facebook_posts_for_tagging",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "tagging_runs/year_filter=2021/month_filter=1/"
                "facebook_posts/for_tagging/facebook_posts_for_tagging.parquet"
            ),
        ),
        (
            "tagging_runs-pipeline_base",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "facebook_posts"},
            "tagging_runs/year_filter=2021/month_filter=1/facebook_posts/",
        ),
    ],
)
def test_tagging_runs_urls(artifact_key, url_config, expected_url):
    """Test tagging runs urls."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    art_url_reg = registry.ArtifactURLRegistry(run_dt, environment_key)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)