"""Test forming run params for finalising accounts for objects."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag.finalise_run_params import accounts


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "youtube_videos"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    ("accounts_final_url, objects_accounts_classes_final_url"),
    [(None, None), ("some_url", "some_url_2")],
)
def test_create(
    accounts_final_url,
    objects_accounts_classes_final_url,
    tenants_template_url_mock,
):
    """Test creating run params for finalising account related data."""
    run_params = accounts.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2022,
        month_filter=1,
        accounts_final_url=accounts_final_url,
        objects_accounts_classes_final_url=objects_accounts_classes_final_url,
    )
    assert run_params
    assert isinstance(run_params, accounts.AccountsFinaliseRunParams)
    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2022/month_filter=1/youtube_videos/"
    )

    urls = run_params.urls
    assert urls.input_objects_dataset == f"{TAGGING_BASE}youtube_videos_pulled.parquet"
    assert urls.input_accounts_classes == (
        "s3://data-lake/tenant_id_1/config/sflm/youtube_videos_account_labels.parquet"
    )

    assert urls.tagging_runs_accounts_final == f"{TAGGING_BASE}accounts_final.parquet"
    assert (
        urls.tagging_runs_objects_accounts_classes_final
        == f"{TAGGING_BASE}objects_accounts_classes_final.parquet"
    )

    if accounts_final_url is None:
        assert urls.accounts_final == ("s3://data-lake/tenant_id_1/final/youtube_videos_accounts/")
    else:
        assert urls.accounts_final == "some_url"

    if objects_accounts_classes_final_url is None:
        assert urls.objects_accounts_classes_final == (
            "s3://data-lake/tenant_id_1/final/youtube_videos_objects_accounts_classes/"
        )
    else:
        assert urls.objects_accounts_classes_final == "some_url_2"
