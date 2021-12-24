"""StartSentimentRunParams."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag.third_party_models.aws_async import start_sentiment_run_params


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "facebook_posts"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"
AWS_DATA_ACCESS_ROLE = "env_aws_data_access_role"


@mock.patch.dict(
    os.environ,
    {
        registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX,
        start_sentiment_run_params.ENV_KEY_AWS_DATA_ACCESS_ROLE: AWS_DATA_ACCESS_ROLE,
    },
)
@pytest.mark.parametrize(
    ("aws_data_access_role, expected_aws_data_access_role"),
    [
        (None, AWS_DATA_ACCESS_ROLE),
        ("", AWS_DATA_ACCESS_ROLE),
        ("arg", "arg"),
        ("arg", "arg"),
    ],
)
def test_create(
    aws_data_access_role,
    expected_aws_data_access_role,
    tenants_template_url_mock,
):
    """Test create of the StartSentimentRunParams."""
    run_params = start_sentiment_run_params.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2021,
        month_filter=11,
        aws_data_access_role=aws_data_access_role,
    )

    assert run_params
    assert isinstance(run_params, start_sentiment_run_params.StartSentimentRunParams)
    assert run_params.aws_data_access_role == expected_aws_data_access_role

    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2021/month_filter=11/facebook_posts/"
    )

    urls = run_params.urls
    assert urls.objects == f"{TAGGING_BASE}objects.parquet"
    assert urls.async_job_group == f"{TAGGING_BASE}sentiment_analysis/async_job_group.json"
    assert urls.comprehend_base == f"{TAGGING_BASE}sentiment_analysis/comprehend_jobs/"


def test_get_aws_data_access_role_execption():
    """Test the _get_aws_data_access_role raises expection if no AWS_DATA_ACCESS_ROLE."""
    with pytest.raises(RuntimeError) as err:
        start_sentiment_run_params._get_aws_data_access_role(None)

    assert start_sentiment_run_params.ENV_KEY_AWS_DATA_ACCESS_ROLE in str(err.value)
