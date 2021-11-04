"""Test pull_label_sheet."""
import pandas as pd

from phoenix.tag.labeling import pull_label_sheet
from phoenix.tag.labeling.generate_label_sheet import EXPECTED_COLUMNS_OBJECT_LABELING_SHEET


def test_is_valid_labeling_sheet():
    correct_df = pd.DataFrame(columns=EXPECTED_COLUMNS_OBJECT_LABELING_SHEET)
    assert pull_label_sheet.is_valid_labeling_sheet(correct_df)


def test_is_valid_labeling_sheet_false():
    incorrect_df = pd.DataFrame(columns=["some_col"])
    assert not pull_label_sheet.is_valid_labeling_sheet(incorrect_df)
