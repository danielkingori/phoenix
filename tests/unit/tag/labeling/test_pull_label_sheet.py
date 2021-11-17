"""Test pull_label_sheet."""
import mock
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


@mock.patch("phoenix.tag.labeling.pull_label_sheet.language.execute")
def test_extract_features_to_label_mapping(mock_execute):
    mock_execute.return_value = pd.DataFrame(data=[("en", 99.5)] * 7)
    input_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2", "id_3"],
            "text": ["this thing speaks woof and bark", "this goes meow", "this is alive"],
            "label_1": ["dog", "cat", "animal"],
            "label_1_features": ["speaks woof,bark", "meow", "alive"],
            "label_2": ["animal", "animal", None],
            "label_2_features": ["speaks woof,bark", None, None],
        },
        columns=EXPECTED_COLUMNS_OBJECT_LABELING_SHEET,
    )

    expected_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_1", "id_1", "id_1", "id_2", "id_3"],
            "class": ["dog", "dog", "animal", "animal", "cat", "animal"],
            "unprocessed_features": [
                "speaks woof",
                "bark",
                "speaks woof",
                "bark",
                "meow",
                "alive",
            ],
            "language": ["en"] * 6,
            "language_confidence": [99.5] * 6,
            "processed_features": ["speak woof", "bark", "speak woof", "bark", "meow", "aliv"],
            "use_processed_features": [True] * 6,
            "status": ["active"] * 6,
        },
        columns=[
            "object_id",
            "class",
            "unprocessed_features",
            "language",
            "language_confidence",
            "processed_features",
            "use_processed_features",
            "status",
        ],
    )

    actual_df, _ = pull_label_sheet.extract_features_to_label_mapping_objects(input_df)

    pd.testing.assert_frame_equal(
        actual_df.sort_values(by=["object_id", "class"]).reset_index(drop=True),
        expected_df.sort_values(by=["object_id", "class"]).reset_index(drop=True),
    )


@mock.patch("phoenix.tag.labeling.pull_label_sheet.language.execute")
def test_extract_features_to_label_mapping_no_features(mock_execute):
    mock_execute.return_value = pd.DataFrame(data=[("en", 99.5)] * 7)
    input_df = pd.DataFrame(
        {
            "object_id": ["note to user about the object_id", "id_1", "id_2", "id_3"],
            "text": [
                "note to user about the text",
                "this thing speaks woof and bark",
                "this goes meow",
                "this is alive",
            ],
            "label_1": ["note to user about label_1", "dog", "cat", "animal"],
            "label_1_features": [
                "note to user about label_1_features",
                "speaks woof,bark",
                "meow",
                "alive",
            ],
            "label_2": ["note to user about label_2", "animal", "animal", None],
            "label_2_features": [
                "note to user about label_2_features",
                "speaks woof,bark",
                None,
                None,
            ],
        },
        columns=EXPECTED_COLUMNS_OBJECT_LABELING_SHEET,
    )

    expected_df = pd.DataFrame(
        {
            "object_id": ["id_2"],
            "class": ["animal"],
            "unprocessed_features": [""],
            "text": ["this goes meow"],
            "language": ["en"],
            "language_confidence": [99.5],
            "processed_features": [""],
            "use_processed_features": [True],
            "status": ["active"],
        },
        columns=[
            "object_id",
            "class",
            "unprocessed_features",
            "text",
            "language",
            "language_confidence",
            "processed_features",
            "use_processed_features",
            "status",
        ],
    )

    _, actual_df = pull_label_sheet.extract_features_to_label_mapping_objects(input_df)

    pd.testing.assert_frame_equal(
        actual_df.sort_values(by=["object_id", "class"]).reset_index(drop=True),
        expected_df.sort_values(by=["object_id", "class"]).reset_index(drop=True),
    )


def test_get_account_labels():
    """Test that get_account_labels returns correct df."""
    input_df = pd.DataFrame(
        {
            "object_user_name": [
                "user_1",
                "user_2",
                "user_3",
            ],
            "object_user_url": [
                "https://www.facebook.com/user_1",
                "https://www.facebook.com/user_2",
                "https://www.facebook.com/user_3",
            ],
            "account_label_1": ["Bot", "", "Journalist"],
            "account_label_2": ["", "", "Publisher"],
            "account_label_3": ["", "", ""],
            "account_label_4": ["", "", ""],
            "account_label_5": ["", "", ""],
        }
    )

    expected_df = pd.DataFrame(
        {
            "object_user_name": [
                "user_1",
                "user_3",
                "user_3",
            ],
            "object_user_url": [
                "https://www.facebook.com/user_1",
                "https://www.facebook.com/user_3",
                "https://www.facebook.com/user_3",
            ],
            "account_label": ["Bot", "Journalist", "Publisher"],
        },
        index=[0, 10, 11],
    )

    actual_df = pull_label_sheet.get_account_labels(input_df)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)
