"""Tests for graphing processing."""

import pandas as pd

from phoenix.tag.graphing import processing_utilities


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
    output_df = processing_utilities.reduce_concat_classes(input_df, ["id_col"], "class")
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
    output_df = processing_utilities.reduce_concat_classes(input_df, ["id_col"], "class")
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_reduce_concat_classes_multi_index_id():
    """Test reduce and concat classes."""
    input_df = pd.DataFrame(
        {
            "id_col_1": ["id_1", "id_1", "id_1"],
            "id_col_2": [2, 1, 2],
            "one_to_one_col": ["val_1", "val_2", "val_1"],
            "class": ["class_2", "class_2", "class_1"],
        }
    )
    expected_df = pd.DataFrame(
        {
            "id_col_1": ["id_1", "id_1"],
            "id_col_2": [1, 2],
            "one_to_one_col": ["val_2", "val_1"],
            "class": ["class_2", "class_1, class_2"],
        }
    )
    output_df = processing_utilities.reduce_concat_classes(
        input_df, ["id_col_1", "id_col_2"], "class"
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_reduce_concat_classes_duplicate_classes():
    """Test reduce and concat classes when duplicate classes found."""
    input_df = pd.DataFrame(
        {
            "id_col": ["id_1", "id_2", "id_1", "id_1"],
            "one_to_one_col": ["val_1", "val_2", "val_1", "val_1"],
            "class": ["class_2", "class_2", "class_1", "class_1"],
        }
    )
    expected_df = pd.DataFrame(
        {
            "id_col": ["id_1", "id_2"],
            "one_to_one_col": ["val_1", "val_2"],
            "class": ["class_1, class_2", "class_2"],
        }
    )
    output_df = processing_utilities.reduce_concat_classes(input_df, ["id_col"], "class")
    pd.testing.assert_frame_equal(output_df, expected_df)
