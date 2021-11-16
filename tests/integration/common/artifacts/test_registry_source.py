"""Test Artifact Registry returns correct URLs."""
import pytest
from freezegun import freeze_time

from tests.integration.common.artifacts import conftest


@freeze_time("2000-01-01 T01:01:01.000001Z")
@pytest.mark.parametrize(
    "artifact_key, url_config, expected_url",
    [
        (
            "source-posts",
            {},
            ("source_runs/2000-01-01/" "source-posts-20000101T010101.000001Z.json"),
        ),
        (
            "source-facebook_comments",
            {},
            ("source_runs/2000-01-01/" "source-facebook_comments-20000101T010101.000001Z.json"),
        ),
        (
            "source-youtube_channels_from_channel_ids",
            {},
            (
                "source_runs/2000-01-01/"
                "source-youtube_channels_from_channel_ids-20000101T010101.000001Z.json"
            ),
        ),
        (
            "source-notebooks_base",
            {},
            ("source_runs/2000-01-01/output_notebooks/20000101T010101.000001Z/"),
        ),
    ],
)
def test_base_grouped_by(artifact_key, url_config, expected_url):
    """Test base grouped by."""
    art_url_reg = conftest.create_test_art_url_reg()
    result_url = art_url_reg.get_url(artifact_key, url_config)
    assert result_url.endswith(expected_url)
