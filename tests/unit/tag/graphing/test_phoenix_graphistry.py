"""Test Graphistry and related functionality."""
import numpy as np
import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry


def test_fillna_string_type_cols():
    """Test filling string cols with empty string."""

    df = pd.DataFrame({"string_col": ["a", None, np.nan, "b"], "num_col": [None, 1, np.nan, 2]})
    expected_df = pd.DataFrame({"string_col": ["a", "", "", "b"], "num_col": [None, 1, np.nan, 2]})
    out_df = phoenix_graphistry.fillna_string_type_cols(df)
    pd.testing.assert_frame_equal(out_df, expected_df)
