"""Test forming run params for finalising inherited accounts for facebook_comments."""
import os

import mock

from phoenix.common.artifacts import registry_environment
from phoenix.tag.finalise_run_params import facebook_comments_inherited_accounts


URL_PREFIX = "s3://data-lake/"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
def test_create(
    tenants_template_url_mock,
):
    """Test creating run params for finalising account related data."""
    run_params = facebook_comments_inherited_accounts.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        year_filter=2022,
        month_filter=1,
    )
    assert run_params
    assert isinstance(
        run_params,
        facebook_comments_inherited_accounts.FacebookCommentsInheritedAccountsFinaliseRunParams,
    )
    TENANT_BASE = "s3://data-lake/tenant_id_1/"
    TAGGING_BASE = TENANT_BASE + "tagging_runs/year_filter=2022/month_filter=1/"

    urls = run_params.urls
    assert urls.facebook_comments_final == (
        f"{TENANT_BASE}"
        "tagging_runs/year_filter=2022/month_filter=1/"
        "facebook_comments/facebook_comments_final.parquet"
    )
    assert urls.facebook_posts_objects_accounts_classes == (
        TAGGING_BASE + "facebook_posts/objects_accounts_classes_final.parquet"
    )
    assert urls.objects_accounts_classes_final == (
        f"{TENANT_BASE}" "final/facebook_comments_inherited_objects_accounts_classes/"
    )
    assert urls.tagging_runs_objects_accounts_classes_final == (
        TAGGING_BASE + "facebook_comments/objects_accounts_classes_final.parquet"
    )
