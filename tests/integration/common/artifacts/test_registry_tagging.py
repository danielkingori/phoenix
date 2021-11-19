"""Test Artifact Registry returns correct URLs."""
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common.artifacts import registry_environment
from tests.integration.common.artifacts import conftest


@freeze_time("2000-01-01 T01:01:01.000001Z")
@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: "s3://data-lake/"})
@pytest.mark.parametrize(
    "artifact_key, url_config, environment_key, expected_url, tenant_id",
    [
        (
            "tagging_runs-facebook_posts_input",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "/test_tenant/"
                "base/grouped_by_year_month/facebook_posts/year_filter=2021/month_filter=1/"
            ),
            "test_tenant",
        ),
        (
            "tagging_runs-facebook_posts_pulled",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "test_tenant/tagging_runs/year_filter=2021/month_filter=1/"
                "facebook_posts/facebook_posts_pulled.parquet"
            ),
            "test_tenant",
        ),
        (
            "tagging_runs-facebook_posts_for_tagging",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "test_tenant/tagging_runs/year_filter=2021/month_filter=1/"
                "facebook_posts/for_tagging/facebook_posts_for_tagging.parquet"
            ),
            "test_tenant",
        ),
        (
            "tagging_runs-youtube_videos_input",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "/test_tenant/"
                "base/grouped_by_year_month/youtube_search_videos/year_filter=2021/month_filter=1/"
            ),
            "test_tenant",
        ),
        (
            "tagging_runs-youtube_videos_pulled",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "test_tenant/tagging_runs/year_filter=2021/month_filter=1/"
                "youtube_videos/youtube_videos_pulled.parquet"
            ),
            "test_tenant",
        ),
        (
            "tagging_runs-youtube_videos_for_tagging",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "local",
            (
                "test_tenant/tagging_runs/year_filter=2021/month_filter=1/"
                "youtube_videos/for_tagging/youtube_videos_for_tagging.parquet"
            ),
            "test_tenant",
        ),
        (
            "tagging_runs-pipeline_base",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "facebook_posts"},
            "local",
            "test_tenant/tagging_runs/year_filter=2021/month_filter=1/facebook_posts/",
            "test_tenant",
        ),
        (
            "tagging_runs-output_notebook_base",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "facebook_posts"},
            "production",
            (
                "s3://data-lake/test_tenant/"
                "tagging_runs/year_filter=2021/month_filter=1/facebook_posts/"
                "output_notebooks/20000101T010101.000001Z/"
            ),
            "test_tenant",
        ),
    ],
)
def test_tagging_runs_urls(artifact_key, url_config, environment_key, expected_url, tenant_id):
    """Test tagging runs urls."""
    art_url_reg = conftest.create_test_art_url_reg(environment_key, tenant_id)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
