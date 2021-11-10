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


def test_get_goal_number_rows():
    input_df = pd.DataFrame(
        data={
            "data_col": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            "stratify_col": ["a", "a", "a", "a", "a", "b", "b", "b", "b", "c"],
        }
    )
    expected_df = pd.DataFrame(
        data={
            "data_col": ["1", "3", "4", "6", "9"],
            "stratify_col": ["a", "a", "a", "b", "b"],
        },
        index=[0, 2, 3, 5, 8],
    )
    excluded_df, actual_df = generate_label_sheet.get_goal_number_rows(input_df, "stratify_col", 5)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
