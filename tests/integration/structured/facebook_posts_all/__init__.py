"""Integration test for facebook_posts_all."""
import pytest

from phoenix.common import utils


@pytest.fixture
def raw_test_data_url() -> str:
    """URL of the raw test data."""
    url = utils.relative_path("./data/raw_data_1.json", __file__)
    return f"file://{url}"


def test_execute(raw_test_data_url):
    """Test execute of the facebook_posts_all.transform."""
    assert False
