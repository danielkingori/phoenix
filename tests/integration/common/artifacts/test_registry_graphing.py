"""Test Artifact Registry returns correct URLs."""
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common import run_datetime
from phoenix.common.artifacts import registry, registry_environment


@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url",
    [
        (
            "graphing_runs-retweet_pulled",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "tweets"},
            (
                "tagging_runs/year_filter=2021/month_filter=1/"
                "tweets/graphing/retweet_pulled.parquet"
            ),
        ),
    ],
)
def test_graphing(artifact_key, url_config, expected_url):
    """Test graphing."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = "local"
    art_url_reg = registry.ArtifactURLRegistry(run_dt, environment_key)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)


@freeze_time("2000-01-01 T01:01:01.000001Z")
@mock.patch.dict(
    os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: "s3://dashboard/"}
)
@pytest.mark.parametrize(
    "artifact_key, url_config, environment, expected_url",
    [
        (
            # For local we do not save to the dashbard
            "graphing_runs-retweet_dashboard_graph",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "tweets"},
            "local",
            None,
        ),
        (
            "graphing_runs-retweet_dashboard_graph",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "tweets"},
            "production",
            (
                "s3://dashboard/"
                "tagging_runs/year_filter=2021/month_filter=1/"
                "tweets/graphing/retweet_graph.html"
            ),
        ),
    ],
)
def test_graphing_dashboard(artifact_key, url_config, environment, expected_url):
    """Test graphing."""
    run_dt = run_datetime.create_run_datetime_now()
    environment_key: registry_environment.Environments = environment
    art_url_reg = registry.ArtifactURLRegistry(run_dt, environment_key)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url == expected_url