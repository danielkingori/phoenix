"""Unit tests for process_annotations."""

import pandas as pd
import pytest

from phoenix.custom_models.tension_classifier import process_annotations


def test_update_column_names():
    input_df = pd.DataFrame([], columns=range(35))
    output_df = process_annotations.update_column_names(input_df)
    assert (output_df.columns == process_annotations.COLUMN_NAMES_ANNOTATIONS).all()


def test_update_column_names_fail():
    input_df = pd.DataFrame([], columns=range(10))
    with pytest.raises(AssertionError):
        _ = process_annotations.update_column_names(input_df)
