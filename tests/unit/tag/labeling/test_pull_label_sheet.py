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


def test_wide_to_long_labels_features():
    input_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2", "id_3"],
            "label_1": ["dog", "cat", "animal"],
            "label_1_features": ["speaks woof,bark", "meow", "alive"],
            "label_2": ["animal", "animal", None],
            "label_2_features": ["speaks woof,bark", None, None],
        },
        columns=[
            "object_id",
            "label_1",
            "label_1_features",
            "label_2",
            "label_2_features",
            "label_3",
            "label_3_features",
            "label_4",
            "label_4_features",
            "label_5",
            "label_5_features",
        ],
    )

    expected_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_1", "id_2", "id_3", "id_1", "id_1", "id_2"],
            "class": ["dog", "dog", "cat", "animal", "animal", "animal", "animal"],
            "unprocessed_features": [
                "speaks woof",
                "bark",
                "meow",
                "alive",
                "speaks woof",
                "bark",
                "",
            ],
        },
        columns=["object_id", "class", "unprocessed_features"],
    )

    actual_df = pull_label_sheet.wide_to_long_labels_features(input_df)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
