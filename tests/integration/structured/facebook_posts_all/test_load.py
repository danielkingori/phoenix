"""Test Transform."""
import pandas as pd

from phoenix.common import artifacts
from phoenix.structured.facebook_posts_all import load


def test_load(tmp_path, expected_facebook_posts_all):
    """Test get_files_in_directory."""
    test_dataset = tmp_path / "facebook_posts_all/"
    result = load.execute(test_dataset, expected_facebook_posts_all)
    assert result

    get_result = artifacts.dask_dataframes.get(test_dataset)
    pd.testing.assert_frame_equal(get_result.dataframe.compute(), expected_facebook_posts_all)
