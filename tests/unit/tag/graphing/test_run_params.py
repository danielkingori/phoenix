"""Test forming run params for graphing."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag.graphing import run_params as graphing_run_params


URL_PREFIX = "s3://data-lake/"
URL_PREFIX_PUBLIC_BUCKET = "s3://public-bucket/"
OBJECT_TYPE = "tweets"
GRAPH_TYPE = "retweet"
INPUT_DATASETS_ARTIFACT_KEYS = ["final-accounts", "tagging_runs-tweets_final"]
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@mock.patch.dict(
    os.environ, {registry_environment.PRODUCTION_DASHBOARD_ENV_VAR_KEY: URL_PREFIX_PUBLIC_BUCKET}
)
@pytest.mark.parametrize(
    ("edges_url, nodes_url, graphistry_redirect_html_url"),
    [(None, None, None), ("some_url", "some_url_2", "some_url_3")],
)
def test_create(
    edges_url,
    nodes_url,
    graphistry_redirect_html_url,
    tenants_template_url_mock,
):
    """Test creating run params for finalising account related data."""
    run_params = graphing_run_params.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2022,
        month_filter=1,
        graph_type=GRAPH_TYPE,
        input_datasets_artifact_keys=INPUT_DATASETS_ARTIFACT_KEYS,
        edges_url=edges_url,
        nodes_url=nodes_url,
        graphistry_redirect_html_url=graphistry_redirect_html_url,
    )
    assert run_params
    assert isinstance(run_params, graphing_run_params.GraphingRunParams)
    ROOT_GRAPH_URL = "s3://data-lake/tenant_id_1/graphing/tweets_retweet/"
    ROOT_FINAL_URL = "s3://data-lake/tenant_id_1/final/"
    ROOT_TAGGING_URL = "s3://data-lake/tenant_id_1/tagging_runs/year_filter=2022/month_filter=1/"

    urls = run_params.urls
    assert urls.input_datasets == {
        "final-accounts": ROOT_FINAL_URL + "tweets_accounts/accounts_final.parquet",
        "tagging_runs-tweets_final": ROOT_TAGGING_URL + "tweets/tweets_final.parquet",
    }

    if edges_url is None:
        assert urls.edges == ROOT_GRAPH_URL + "edges.parquet"
    else:
        assert urls.edges == "some_url"
    if nodes_url is None:
        assert urls.nodes == ROOT_GRAPH_URL + "nodes.parquet"
    else:
        assert urls.nodes == "some_url_2"
    if graphistry_redirect_html_url is None:
        assert urls.graphistry_redirect_html == (
            "s3://public-bucket/tenant_id_1/graphing/tweets_retweet/graphistry/redirect.html"
        )
    else:
        assert urls.graphistry_redirect_html == "some_url_3"
