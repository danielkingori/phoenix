"""Test generate_label_sheet."""
import pandas as pd

from phoenix.tag.labeling import generate_label_sheet


def test_create_new_labeling_sheet_empty():
    empty_df = pd.DataFrame()
    actual_df = generate_label_sheet.create_new_labeling_sheet_df(empty_df)

    assert all(actual_df.columns.values == generate_label_sheet.EXPECTED_COLUMNS_LABELING_SHEET)
    assert actual_df.empty


def test_create_new_labeling_sheet():
    df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2"],
            "text": ["text_1", "text_2"],
            "column_to_be_ignore": ["wrong_text_1", "wrong_text_2"],
        }
    )

    expected_df = pd.DataFrame(
        {"object_id": ["id_1", "id_2"], "text": ["text_1", "text_2"]},
        columns=generate_label_sheet.EXPECTED_COLUMNS_LABELING_SHEET,
    )

    actual_df = generate_label_sheet.create_new_labeling_sheet_df(df)

    pd.testing.assert_frame_equal(actual_df, expected_df)
