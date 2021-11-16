"""Test Artifact Registry returns correct URLs."""
import pytest
from freezegun import freeze_time

from tests.integration.common.artifacts import conftest


@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url",
    [
        (
            "base-grouped_by_posts",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/facebook_posts/"
                "year_filter=2021/month_filter=1/posts-20000101T010101.000001Z.json"
            ),
        ),
        (
            "base-grouped_by_user_tweets",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/tweets/"
                "year_filter=2021/month_filter=1/user_tweets-20000101T010101.000001Z.json"
            ),
        ),
        (
            "base-grouped_by_keyword_tweets",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/tweets/"
                "year_filter=2021/month_filter=1/keyword_tweets-20000101T010101.000001Z.json"
            ),
        ),
        (
            "base-grouped_by_facebook_comments",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/facebook_comments/"
                "year_filter=2021/month_filter=1/facebook_comments-20000101T010101.000001Z.json"
            ),
        ),
        (
            "base-facebook_comments_pages_to_parse",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/facebook_comments_pages/"
                "year_filter=2021/month_filter=1/to_parse/"
            ),
        ),
        (
            "base-facebook_comments_pages_successful_parse",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/facebook_comments_pages/"
                "year_filter=2021/month_filter=1/successful_parse/20000101T010101.000001Z/"
            ),
        ),
        (
            "base-facebook_comments_pages_failed_parse",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/facebook_comments_pages/"
                "year_filter=2021/month_filter=1/failed_parse/20000101T010101.000001Z/"
            ),
        ),
        (
            "base-grouped_by_youtube_channels",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/youtube_channels/"
                "year_filter=2021/month_filter=1/youtube_channels-20000101T010101.000001Z.json"
            ),
        ),
        (
            "base-grouped_by_youtube_searches",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            (
                "base/grouped_by_year_month/youtube_searches/"
                "year_filter=2021/month_filter=1/"
                "youtube_video_searches-20000101T010101.000001Z.json"
            ),
        ),
    ],
)
def test_base_grouped_by(artifact_key, url_config, expected_url):
    """Test base grouped by."""
    art_url_reg = conftest.create_test_art_url_reg()
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
