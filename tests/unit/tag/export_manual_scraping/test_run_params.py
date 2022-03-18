"""Run params for export manual scraping."""
import os

import mock
import pytest

from phoenix.common.artifacts import registry_environment
from phoenix.tag import export_manual_scraping


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "facebook_posts"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    (
        "include_accounts, expected_include_accounts"
        ", has_topics, expected_has_topics"
        ", custom_prefix, expected_facebook_posts_to_scrape_url"
        ", head, expected_head"
    ),
    [
        (
            None,
            None,
            None,
            True,
            None,
            "default",
            None,
            export_manual_scraping.run_params.DEFAULT_HEAD,
        ),
        (
            None,
            None,
            True,
            True,
            "some_prefix-",
            (
                "s3://data-lake/tenant_id_1/"
                "tagging_runs/year_filter=2021/month_filter=11/facebook_posts/"
                "export_manual_scraping/some_prefix-facebook_posts_to_scrape.csv"
            ),
            20,
            20,
        ),
        (None, None, False, False, None, "default", "30", 30),
        ("account_1", ["account_1"], "t", True, None, "default", 10, 10),
        (["account_1"], ["account_1"], "True", True, None, "default", 10, 10),
        (
            "account_1,account_2",
            ["account_1", "account_2"],
            "FALSE",
            False,
            None,
            "default",
            10,
            10,
        ),
        (
            ["account_1", "account_2"],
            ["account_1", "account_2"],
            "f",
            False,
            None,
            "default",
            10,
            10,
        ),
    ],
)
def test_create(
    include_accounts,
    expected_include_accounts,
    has_topics,
    expected_has_topics,
    custom_prefix,
    expected_facebook_posts_to_scrape_url,
    head,
    expected_head,
    tenants_template_url_mock,
):
    """Test create of the export manual scraping run params."""
    run_params = export_manual_scraping.run_params.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        object_type=OBJECT_TYPE,
        year_filter=2021,
        month_filter=11,
        include_accounts=include_accounts,
        has_topics=has_topics,
        custom_prefix=custom_prefix,
        head=head,
    )

    assert run_params
    assert isinstance(run_params, export_manual_scraping.run_params.ExportManualScrapingRunParams)
    assert run_params.include_accounts == expected_include_accounts
    assert run_params.has_topics == expected_has_topics
    assert run_params.head == expected_head

    TAGGING_BASE = (
        "s3://data-lake/tenant_id_1/"
        "tagging_runs/year_filter=2021/month_filter=11/facebook_posts/"
    )

    urls = run_params.urls
    assert urls.input_dataset == f"{TAGGING_BASE}facebook_posts_final.parquet"
    if expected_facebook_posts_to_scrape_url == "default":
        assert (
            urls.custom_facebook_posts_to_scrape
            == f"{TAGGING_BASE}export_manual_scraping/facebook_posts_to_scrape.csv"
        )
    else:
        assert urls.custom_facebook_posts_to_scrape == expected_facebook_posts_to_scrape_url
