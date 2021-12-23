"""DataPullRunParams for Facebook posts."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag import data_pull


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "youtube_comments"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    (
        "year_filter, expected_year_filter"
        ",month_filter, expected_month_filter"
        ",include_all_data_for_month, expected_include_all_data_for_month"
    ),
    [
        (2021, 2021, 11, 11, None, False),
        (2021, 2021, 11, 11, "f", False),
        (2021, 2021, 11, 11, "False", False),
        (2021, None, 11, None, "T", True),
        (2021, None, 11, None, "TRUE", True),
        (2021, None, 11, None, True, True),
    ],
)
def test_create(
    year_filter,
    expected_year_filter,
    month_filter,
    expected_month_filter,
    include_all_data_for_month,
    expected_include_all_data_for_month,
    tenants_template_url_mock,
):
    """Test create of the youtube_comments data pull run params."""
    run_params = data_pull.run_params.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2021,
        month_filter=11,
        include_all_data_for_month=include_all_data_for_month,
    )

    assert run_params
    assert isinstance(run_params, data_pull.run_params.dtypes.DataPullRunParams)
    assert run_params.include_all_data_for_month == expected_include_all_data_for_month

    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2021/month_filter=11/youtube_comments/"
    )

    urls = run_params.urls
    assert urls.for_tagging == f"{TAGGING_BASE}for_tagging/youtube_comments_for_tagging.parquet"

    assert urls.pulled == f"{TAGGING_BASE}youtube_comments_pulled.parquet"

    input_dataset = (
        "s3://data-lake/tenant_id_1/"
        "base/grouped_by_year_month/youtube_comment_threads/year_filter=2021/month_filter=11/"
    )

    assert urls.input_dataset == input_dataset
