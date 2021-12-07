"""Test facebook_posts_all flow."""
import logging

import pandas as pd
import pytest
from prefect.utilities.debug import raise_on_exception

from phoenix.common import artifacts, utils
from phoenix.structured import facebook_posts_all


@pytest.fixture
def raw_test_data_dir_url():
    """Get URL for raw test data."""
    test_folder = utils.relative_path("./data/", __file__)
    return f"file://{test_folder}"


def test_flow(raw_test_data_dir_url, expected_facebook_posts_all, tmp_path):
    """Test flow."""
    test_dataset = tmp_path / "facebook_posts_all/"
    # Debugging the state
    with raise_on_exception():
        state = facebook_posts_all.flow.run(
            run_on_schedule=False, input_dir=raw_test_data_dir_url, output_dir=test_dataset
        )
    assert state
    assert state.is_successful()
    get_result = artifacts.dask_dataframes.get(test_dataset)
    assert get_result
    persisted_df = get_result.dataframe.compute()
    # The two files that are processed are the same with different file names
    expected_df = pd.concat([expected_facebook_posts_all] * 2)
    # Compare the file_urls
    file_base = "phoenix/tests/integration/structured/facebook_posts_all/data/"
    file_urls = [
        f"{file_base}raw_data_1-20210202T010101.000001Z.json",
        f"{file_base}raw_data_1-20210302T010101.000001Z.json",
    ].sort()
    assert file_urls == persisted_df["file_url"].unique().sort()
    # Drop the file columns that will be different
    persisted_df = persisted_df.drop(columns=["file_url", "file_base", "file_timestamp"])
    expected_df = expected_df.drop(columns=["file_base", "file_timestamp"])
    # Comparing by col for debugging
    for col in persisted_df.columns:
        logging.info(col)
        pd.testing.assert_series_equal(persisted_df[col], expected_df[col])
    pd.testing.assert_frame_equal(persisted_df, expected_df)
