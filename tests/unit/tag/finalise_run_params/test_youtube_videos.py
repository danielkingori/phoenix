"""FinaliseRunParams for youtube videos."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag import finalise_run_params


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "youtube_videos"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    (
        "include_objects_tensions, expected_include_objects_tensions"
        ",include_sentiment, expected_include_sentiment"
        ",final_url, expected_final_url"
    ),
    [
        (False, False, True, True, None, "default"),
        ("False", False, "True", True, None, "default"),
        ("F", False, "t", True, None, "default"),
        ("", False, None, False, None, "default"),
        ("", False, None, False, "some_url", "some_url"),
    ],
)
def test_create(
    include_objects_tensions,
    expected_include_objects_tensions,
    include_sentiment,
    expected_include_sentiment,
    final_url,
    expected_final_url,
    tenants_template_url_mock,
):
    """Test create of the youtube_videos run params."""
    run_params = finalise_run_params.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2021,
        month_filter=11,
        final_url=final_url,
        include_objects_tensions=include_objects_tensions,
        include_sentiment=include_sentiment,
    )

    assert run_params
    assert isinstance(run_params, finalise_run_params.dtypes.FinaliseRunParams)
    assert run_params.include_objects_tensions == expected_include_objects_tensions
    assert run_params.include_sentiment == expected_include_sentiment

    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2021/month_filter=11/youtube_videos/"
    )

    urls = run_params.urls
    assert urls.input_dataset == f"{TAGGING_BASE}youtube_videos_pulled.parquet"
    assert urls.objects_tensions == f"{TAGGING_BASE}objects_tensions.parquet"
    assert urls.language_sentiment_objects == f"{TAGGING_BASE}language_sentiment_objects/"
    assert urls.tagging_final == f"{TAGGING_BASE}youtube_videos_final.parquet"
    if expected_final_url == "default":
        FINAL_BASE = "s3://data-lake/tenant_id_1/final/youtube_videos/"
        assert urls.final == FINAL_BASE
    else:
        assert urls.final == expected_final_url
