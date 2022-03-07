"""Test Artifact Registry returns correct URLs."""
import pytest
from freezegun import freeze_time

from tests.integration.common.artifacts import conftest


@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url",
    [
        (
            "final-facebook_posts",
            {"YEAR_FILTER": 2021, "MONTH_FILTER": 1},
            "final/facebook_posts/",
        ),
    ],
)
def test_final(artifact_key, url_config, expected_url):
    """Test final urls."""
    art_url_reg = conftest.create_test_art_url_reg()
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
