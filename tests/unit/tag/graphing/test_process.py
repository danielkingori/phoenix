"""Tests for graphing processing."""

import pandas as pd

from phoenix.tag.graphing import process


def test_reduce_concat_classes():
    """Test reduce and concat classes."""
    input_df = pd.DataFrame(
        {
            "id_col": ["id_1", "id_2", "id_1"],
            "one_to_one_col": ["val_1", "val_2", "val_1"],
            "class": ["class_2", "class_2", "class_1"],
        }
    )
    expected_df = pd.DataFrame(
        {
            "id_col": ["id_1", "id_2"],
            "one_to_one_col": ["val_1", "val_2"],
            "class": ["class_1, class_2", "class_2"],
        }
    )
    output_df = process.reduce_concat_classes(input_df, "id_col", "class")
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_reduce_concat_classes_no_one_to_one_columns():
    """Test reduce and concat class with no one_to_one columns."""
    input_df = pd.DataFrame(
        {
            "id_col": ["id_1", "id_2", "id_1"],
            "class": ["class_2", "class_2", "class_1"],
        }
    )
    expected_df = pd.DataFrame(
        {
            "id_col": ["id_1", "id_2"],
            "class": ["class_1, class_2", "class_2"],
        }
    )
    output_df = process.reduce_concat_classes(input_df, "id_col", "class")
    pd.testing.assert_frame_equal(output_df, expected_df)
