"""Test Artifact Registry returns correct URLs."""
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common.artifacts import registry_environment
from tests.integration.common.artifacts import conftest


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
    art_url_reg = conftest.create_test_art_url_reg()
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)


@freeze_time("2000-01-01 T01:01:01.000001Z")
@mock.patch.dict(
    os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: "s3://dashboard/"}
)
@pytest.mark.parametrize(
    "artifact_key, url_config, environment, tenant_id, expected_url",
    [
        (
            # For local we do not save to the dashbard
            "graphing_runs-retweet_dashboard_graph",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "tweets"},
            "local",
            "test_tenant",
            None,
        ),
        (
            "graphing_runs-retweet_dashboard_graph",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1, "OBJECT_TYPE": "tweets"},
            "production",
            "test_tenant",
            (
                "s3://dashboard/test_tenant/"
                "tagging_runs/year_filter=2021/month_filter=1/"
                "tweets/graphing/retweet_graph.html"
            ),
        ),
    ],
)
def test_graphing_dashboard(artifact_key, url_config, environment, tenant_id, expected_url):
    """Test graphing."""
    art_url_reg = conftest.create_test_art_url_reg(environment, tenant_id)
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url == expected_url
