"""Unit tests for the sflm_processing module."""
import pandas as pd

from phoenix.tag.labelling import sflm_processing


def test_convert_to_bool():
    """Tests the convert_to_bool function."""
    input_df = pd.DataFrame(
        data={
            "some_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "maybe_bool_col": [True, False, "True", "False", "TRUE", "FALSE", "truuue", "false"],
        }
    )

    expected_df = pd.DataFrame(
        data={
            "some_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "maybe_bool_col": [True, False, True, False, True, False, True, False],
        }
    )

    actual_df = input_df.copy()
    actual_df["maybe_bool_col"] = actual_df["maybe_bool_col"].apply(
        sflm_processing.convert_to_bool
    )
    pd.testing.assert_frame_equal(actual_df, expected_df)
