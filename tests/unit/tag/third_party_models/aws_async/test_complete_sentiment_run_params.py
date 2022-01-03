"""CompleteSentimentRunParams."""
import os

import mock

from phoenix.common.artifacts import registry_environment
from phoenix.tag.third_party_models.aws_async import complete_sentiment_run_params


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "facebook_posts"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
def test_create(
    tenants_template_url_mock,
):
    """Test create of the CompleteSentimentRunParams."""
    run_params = complete_sentiment_run_params.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2021,
        month_filter=11,
    )

    assert run_params
    assert isinstance(run_params, complete_sentiment_run_params.CompleteSentimentRunParams)

    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2021/month_filter=11/facebook_posts/"
    )

    urls = run_params.urls
    assert urls.async_job_group == f"{TAGGING_BASE}sentiment_analysis/async_job_group.json"
    assert urls.language_sentiment_objects == f"{TAGGING_BASE}language_sentiment_objects.parquet"
