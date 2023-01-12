"""Test forming run params for finalising accounts for objects."""
import datetime
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common.artifacts import registry_environment
from phoenix.scrape.run_params import facebook_posts


URL_PREFIX = "s3://data-lake/"
OBJECT_TYPE = "youtube_videos"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@freeze_time("2000-01-1 01:01:01", tz_offset=0)
@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    (
        "scrape_since_days, expected_scrape_since_days"
        ", scrape_start_date, expected_scrape_start_date"
        ", scrape_end_date, expected_scrape_end_date,"
        ", crowdtangle_list_ids, expected_crowdtangle_list_ids"
    ),
    [
        (
            None,
            3,
            None,
            datetime.datetime(1999, 12, 29, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            "id1,id2",
            ["id1", "id2"],
        ),
        (
            "3",
            3,
            None,
            datetime.datetime(1999, 12, 29, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            "id1,id2",
            ["id1", "id2"],
        ),
        (
            4,
            4,
            None,
            datetime.datetime(1999, 12, 28, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
        (
            "0",
            0,
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
        (
            0,
            0,
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
        (
            4,
            4,
            None,
            datetime.datetime(1999, 11, 27, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "1999-12-01",
            datetime.datetime(1999, 12, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
        (
            None,
            3,
            None,
            datetime.datetime(1999, 11, 28, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "1999-12-01",
            datetime.datetime(1999, 12, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
        (
            None,
            None,
            datetime.datetime(1999, 12, 28, 1, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(1999, 12, 28, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
        (
            None,
            None,
            "1999-11-01",
            datetime.datetime(1999, 11, 1, 0, 0, tzinfo=datetime.timezone.utc),
            "1999-12-01",
            datetime.datetime(1999, 12, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            None,
            ["list_id_1"],
        ),
    ],
)
def test_create(
    scrape_since_days,
    expected_scrape_since_days,
    scrape_start_date,
    expected_scrape_start_date,
    scrape_end_date,
    expected_scrape_end_date,
    crowdtangle_list_ids,
    expected_crowdtangle_list_ids,
    tenants_template_url_mock,
):
    """Test creating run params for finalising facebook_posts related data."""
    run_params = facebook_posts.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        scrape_since_days=scrape_since_days,
        scrape_start_date=scrape_start_date,
        scrape_end_date=scrape_end_date,
        crowdtangle_list_ids=crowdtangle_list_ids,
    )
    assert run_params
    assert isinstance(run_params, facebook_posts.FacebookPostsScrapeRunParams)
    BASE = "s3://data-lake/tenant_id_1/"

    urls = run_params.urls
    assert urls.source == f"{BASE}source_runs/2000-01-01/source-posts-20000101T010101.000000Z.json"
    assert run_params.scrape_since_days == expected_scrape_since_days
    assert run_params.scrape_start_date == expected_scrape_start_date
    assert run_params.scrape_end_date == expected_scrape_end_date
    assert run_params.crowdtangle_list_ids == expected_crowdtangle_list_ids


@freeze_time("2000-01-1 01:01:01", tz_offset=0)
@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    ("scrape_since_days" ", scrape_start_date" ", scrape_end_date" ", crowdtangle_list_ids"),
    [
        (
            3,
            "1999-12-28",
            None,
            "id1,id2",
        ),
        (
            "3",
            "1999-12-28",
            None,
            "id1,id2",
        ),
    ],
)
def test_create_value_error_scraping_range(
    scrape_since_days,
    scrape_start_date,
    scrape_end_date,
    crowdtangle_list_ids,
    tenants_template_url_mock,
):
    """Test error thrown when scrape_since_days and scrape_start_date are both set."""
    with pytest.raises(ValueError) as e:
        facebook_posts.create(
            artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
            tenant_id=TENANT_ID,
            run_datetime_str=None,
            scrape_since_days=scrape_since_days,
            scrape_start_date=scrape_start_date,
            scrape_end_date=scrape_end_date,
            crowdtangle_list_ids=crowdtangle_list_ids,
        )
        assert "scrape_since_days" in str(e.value)
        assert "scrape_start_date" in str(e.value)
