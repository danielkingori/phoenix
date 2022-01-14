"""FinaliseRunParams for youtube comments."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag import finalise_run_params


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "youtube_comments"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    ("rename_topic_to_class, expected_rename_topic_to_class" ",final_url, expected_final_url"),
    [
        (False, False, None, "default"),
        ("False", False, None, "default"),
        ("F", False, None, "default"),
        ("", False, None, "default"),
        (False, False, None, "default"),
        ("True", True, None, "default"),
        ("t", True, None, "default"),
        (None, False, None, "default"),
        (None, False, "some_url", "some_url"),
    ],
)
def test_topic_create(
    rename_topic_to_class,
    expected_rename_topic_to_class,
    final_url,
    expected_final_url,
    tenants_template_url_mock,
):
    """Test create of the youtube_comments_topics run params."""
    run_params = finalise_run_params.topics_create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2021,
        month_filter=11,
        final_url=final_url,
        rename_topic_to_class=rename_topic_to_class,
    )

    assert run_params
    assert isinstance(run_params, finalise_run_params.topics_dtypes.TopicsFinaliseRunParams)
    assert run_params.rename_topic_to_class == expected_rename_topic_to_class

    data_set_name = "youtube_comments_topics"

    if rename_topic_to_class:
        data_set_name = "youtube_comments_classes"

    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2021/month_filter=11/youtube_comments/"
    )

    urls = run_params.urls
    assert urls.input_dataset == f"{TAGGING_BASE}youtube_comments_final.parquet"
    assert urls.topics == f"{TAGGING_BASE}topics.parquet"
    assert urls.tagging_final == f"{TAGGING_BASE}youtube_comments_topics_final.parquet"
    if expected_final_url == "default":
        FINAL_BASE = (
            f"s3://data-lake/tenant_id_1/final/{data_set_name}/{data_set_name}_final.parquet"
        )
        assert urls.final == FINAL_BASE
    else:
        assert urls.final == expected_final_url
