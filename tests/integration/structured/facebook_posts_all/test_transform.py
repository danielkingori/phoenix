"""Test Transform."""
import logging

import pandas as pd
import pytest

from phoenix.common import utils
from phoenix.structured.facebook_posts_all import transform


@pytest.fixture
def raw_test_data_url():
    """Get URL for raw test data."""
    test_folder = utils.relative_path("./data/raw_data_1-20210202T010101.000001Z.json", __file__)
    return f"file://{test_folder}"


def test_execute(raw_test_data_url, expected_facebook_posts_all):
    """Test transform execute."""
    result = transform.execute(raw_test_data_url)
    ends_with = (
        "phoenix/tests/integration/structured/facebook_posts_all/"
        "data/raw_data_1-20210202T010101.000001Z.json"
    )
    assert result["file_url"].str.endswith(ends_with).all()
    result = result.drop(columns=["file_url"])
    # Problems with debugging the dates
    # This means that we can check which column is not correct
    for col in result.columns:
        logging.info(col)
        pd.testing.assert_series_equal(result[col], expected_facebook_posts_all[col])
    pd.testing.assert_frame_equal(result, expected_facebook_posts_all)
