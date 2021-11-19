"""Test generate_label_sheet."""
import pandas as pd

from phoenix.tag.labeling import generate_label_sheet


def test_create_new_labeling_sheet_empty():
    empty_df = pd.DataFrame()
    actual_df = generate_label_sheet.create_new_object_labeling_sheet_df(empty_df)

    assert all(
        actual_df.columns.values == generate_label_sheet.EXPECTED_COLUMNS_OBJECT_LABELING_SHEET
    )
    assert len(actual_df) == 1
    pd.testing.assert_frame_equal(actual_df, generate_label_sheet.get_user_notes_object_df())


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
        columns=generate_label_sheet.EXPECTED_COLUMNS_OBJECT_LABELING_SHEET,
    )

    actual_df = generate_label_sheet.create_new_object_labeling_sheet_df(df)

    pd.testing.assert_frame_equal(actual_df[1:], expected_df)


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


def test_get_goal_number_rows_many_unique():
    input_df = pd.DataFrame(
        data={
            "object_id": ["id_1", "id_2", "id_3"],
            "data_col": ["1", "2", "3"],
            "stratify_col": ["a", "b", "c"],
        }
    )

    expected_df = pd.DataFrame(
        data={
            "object_id": ["id_3", "id_2"],
            "data_col": ["3", "2"],
            "stratify_col": ["c", "b"],
        },
        index=[2, 1],
    )
    excluded_df, actual_df = generate_label_sheet.get_goal_number_rows(input_df, "stratify_col", 2)
    pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)


def test_get_user_notes_object_df():
    """Test basic assumptions for get_user_notes_object_df."""
    notes_object_df = generate_label_sheet.get_user_notes_object_df()
    assert len(notes_object_df) == 1

    # Assert that the number of filled cells in the notes is equal to the expected columns number
    assert notes_object_df.iloc[0].count() == len(
        generate_label_sheet.EXPECTED_COLUMNS_OBJECT_LABELING_SHEET
    )
