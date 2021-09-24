"""Test Artifact Registry returns correct URLs."""
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment


@freeze_time("2000-01-01 T01:01:01.000001Z")
@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: "s3://data-lake/"})
@pytest.mark.parametrize(
    "artifact_key, url_config, environment_key, expected_url",
    [
        (
            "tagging_runs-facebook_posts_input",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            "base/grouped_by_year_month/facebook_posts/year_filter=2021/month_filter=1/",
        ),
        (
            "tagging_runs-facebook_posts_pulled",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "tagging_runs/year_filter=2021/month_filter=1/"
                "facebook_posts/facebook_posts_pulled.parquet"
            ),
        ),
        (
            "tagging_runs-facebook_posts_for_tagging",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "tagging_runs/year_filter=2021/month_filter=1/"
                "facebook_posts/for_tagging/facebook_posts_for_tagging.parquet"
            ),
        ),
        (
            "tagging_runs-pipeline_base",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "facebook_posts"},
            "local",
            "tagging_runs/year_filter=2021/month_filter=1/facebook_posts/",
        ),
        (
            "tagging_runs-notebook_base",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "facebook_posts"},
            "production",
            (
                "s3://data-lake/"
                "tagging_runs/year_filter=2021/month_filter=1/facebook_posts/"
                "output_notebooks/20000101T010101.000001Z/"
            ),
        ),
    ],
)
def test_tagging_runs_urls(artifact_key, url_config, environment_key, expected_url):
    """Test tagging runs urls."""
    run_dt = run_datetime.create_run_datetime_now()
    art_url_reg = registry.ArtifactURLRegistry(run_dt, environment_key)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
