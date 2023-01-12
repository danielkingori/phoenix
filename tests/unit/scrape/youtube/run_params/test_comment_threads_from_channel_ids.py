"""Test forming run params for finalising comment_threads_from_channel_ids for objects."""
import os

import mock
import pytest
from freezegun import freeze_time

from phoenix.common.artifacts import registry_environment
from phoenix.scrape.youtube.run_params import comment_threads_from_channel_ids


URL_PREFIX = "s3://data-lake/"
ARTIFACTS_ENVIRONMENT_KEY = "production"
TENANT_ID = "tenant_id_1"


@freeze_time("2000-01-1 01:01:01", tz_offset=0)
@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@pytest.mark.parametrize(
    ("max_pages, expected_max_pages, static_youtube_channels, expected_static_youtube_channels"),
    [
        (
            "3",
            3,
            None,
            None,
        ),
        (
            None,
            10,
            None,
            None,
        ),
        (
            4,
            4,
            "s3://different_static_youtube_channels.csv",
            "s3://different_static_youtube_channels.csv",
        ),
    ],
)
def test_create(
    max_pages,
    expected_max_pages,
    static_youtube_channels,
    expected_static_youtube_channels,
    tenants_template_url_mock,
):
    """Test creating run params for finalising comment_threads_from_channel_ids related data."""
    run_params = comment_threads_from_channel_ids.create(
        artifact_env=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=TENANT_ID,
        run_datetime_str=None,
        max_pages=max_pages,
        static_youtube_channels=static_youtube_channels,
    )
    assert run_params
    assert isinstance(
        run_params, comment_threads_from_channel_ids.ScrapeYouTubeCommentThreadsFromChannelIds
    )
    BASE = "s3://data-lake/tenant_id_1/"

    urls = run_params.urls
    assert urls.source_youtube_comment_threads_from_channel_ids == (
        f"{BASE}"
        "source_runs/2000-01-01/"
        "source-youtube_comment_threads_from_channel_ids-20000101T010101.000000Z.json"
    )
    assert urls.base_youtube_comment_threads == (
        f"{BASE}"
        "base/grouped_by_year_month/youtube_comment_threads/year_filter=2000/month_filter=1/"
        "youtube_comment_threads-20000101T010101.000000Z.json"
    )
    assert run_params.max_pages == expected_max_pages

    if not expected_static_youtube_channels:
        assert urls.static_youtube_channels == (f"{BASE}" "config/youtube_channels.csv")
    else:
        assert urls.static_youtube_channels == expected_static_youtube_channels
