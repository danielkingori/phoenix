"""Test pull_label_sheet."""
import mock
import numpy as np
import pandas as pd
import pytest

from phoenix.tag.labelling import pull_label_sheet
from phoenix.tag.labelling.generate_label_sheet import EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET


def test_is_valid_labelling_sheet():
    correct_df = pd.DataFrame(columns=EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET)
    assert pull_label_sheet.is_valid_labelling_sheet(correct_df)


def test_is_valid_labelling_sheet_false():
    incorrect_df = pd.DataFrame(columns=["some_col"])
    assert not pull_label_sheet.is_valid_labelling_sheet(incorrect_df)


def test_wide_to_long_labels_features():
    input_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2", "id_3"],
            "label_1": ["       dog", "cat        ", "Animal\n"],
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


@pytest.fixture
def labelled_data() -> pd.DataFrame:
    """Manually labelled data."""
    df = pd.DataFrame(
        {
            "object_id": ["note to user about the object_id", "id_1", "id_2", "id_3"],
            "text": [
                "note to user about the text",
                "this thing speaks woof and bark\n\n\nbark",
                "this goes meow",
                "this is alive eukaryotic",
            ],
            "label_1": ["note to user about label_1", "dog", "cat", "animal"],
            "label_1_features": [
                "note to user about label_1_features",
                "speaks woof,bark",
                "meow",
                # the following line contains the arabic comma "،"
                "alive، eukaryotic",
            ],
            "label_2": ["note to user about label_2", "animal", "animal", None],
            "label_2_features": [
                "note to user about label_2_features",
                "speaks woof,bark",
                None,
                None,
            ],
        },
        columns=EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET,
    )
    return df


@pytest.fixture
def single_feature_to_label_mapping() -> pd.DataFrame:
    """Single feature to label mapping df that corresponds to labelled data fixture."""
    df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_1", "id_1", "id_1", "id_2", "id_3", "id_3"],
            "class": ["dog", "dog", "animal", "animal", "cat", "animal", "animal"],
            "unprocessed_features": [
                "speaks woof",
                "bark",
                "speaks woof",
                "bark",
                "meow",
                "alive",
                "eukaryotic",
            ],
            "language": ["en"] * 7,
            "language_confidence": [99.5] * 7,
            "processed_features": [
                "speak woof",
                "bark",
                "speak woof",
                "bark",
                "meow",
                "aliv",
                "eukaryot",
            ],
            "use_processed_features": [False] * 7,
            "status": ["active"] * 7,
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
    return df


def test_compute_sflm_statistics(labelled_data, single_feature_to_label_mapping):
    """Test computing statistics for sflm."""
    sflm_statistics_df = pull_label_sheet.compute_sflm_statistics(
        labelled_data, single_feature_to_label_mapping
    )
    expected_df = pd.DataFrame(
        {
            "class": ["animal", "cat", "dog"],
            "num_features": [4, 1, 2],
            "num_objects_labelled": [3, 1, 1],
        }
    )
    pd.testing.assert_frame_equal(sflm_statistics_df, expected_df)


@mock.patch("phoenix.tag.labelling.pull_label_sheet.language.execute")
def test_extract_features_to_label_mapping(
    mock_execute, labelled_data, single_feature_to_label_mapping
):
    mock_execute.return_value = pd.DataFrame(data=[("en", 99.5)] * 8)

    actual_df, _ = pull_label_sheet.extract_features_to_label_mapping_objects(labelled_data)

    pd.testing.assert_frame_equal(
        actual_df.sort_values(by=["object_id", "class"]).reset_index(drop=True),
        single_feature_to_label_mapping.sort_values(by=["object_id", "class"]).reset_index(
            drop=True
        ),
    )


@mock.patch("phoenix.tag.labelling.pull_label_sheet.language.execute")
def test_extract_features_to_label_mapping_no_features(mock_execute):
    mock_execute.return_value = pd.DataFrame(data=[("en", 99.5)] * 8)
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
            "label_3": ["note to user about label_3", None, "animal", None],
        },
        columns=EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET,
    )

    expected_df = pd.DataFrame(
        {
            "object_id": ["id_2"],
            "class": ["animal"],
            "text": ["this goes meow"],
            "language": ["en"],
            "language_confidence": [99.5],
        },
        columns=[
            "object_id",
            "class",
            "text",
            "language",
            "language_confidence",
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
            "labelled_by": ["Andrew", "Andrew", "Andrew"],
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
            "labelled_by": ["Andrew", "Andrew", "Andrew"],
            "account_label": ["Bot", "Journalist", "Publisher"],
        },
        index=[0, 10, 11],
    )

    actual_df = pull_label_sheet.get_account_labels(input_df)

    pd.testing.assert_frame_equal(actual_df, expected_df, check_like=True)


def test_clean_feature_to_label_df():
    """Test the clean_feature_to_label_df outputs correct cols and deduplicates.

    Dedupe is based on class + unprocessed_features + processed_features
    """
    input_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2", "id_3", "id_4"],
            "class": ["cat", "dog", "cat", "dog"],
            "unprocessed_features": ["meow", "woof", "meow", np.nan],
            "text": ["meow meow", "bark woof", "say meow", "faithful furry friend"],
            "language": ["en"] * 4,
            "language_confidence": [0.99] * 4,
            "processed_features": ["meow", "woof", "meow", ""],
        }
    )

    expected_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2"],
            "class": ["cat", "dog"],
            "unprocessed_features": ["meow", "woof"],
            "language": ["en"] * 2,
            "language_confidence": [0.99] * 2,
            "processed_features": ["meow", "woof"],
            "use_processed_features": [False] * 2,
            "status": ["active"] * 2,
        }
    )

    actual_df = pull_label_sheet.clean_feature_to_label_df(input_df)

    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_extract_labelled_examples_with_no_feature():
    """Test that labelled examples with no feature are extracted from the feature_to_label_df."""
    input_df = pd.DataFrame(
        data={
            "object_id": ["id_1", "id_1", "id_2", "id_2", "id_3"],
            "class": [
                "doubly_filled_in_class_user_error",
                "doubly_filled_in_class_user_error",
                "cat",
                "animal",
                "animal",
            ],
            "unprocessed_features": ["", "", "meow", "", "eukaryote"],
            "text": [
                "furry best friend",
                "furry best friend",
                "i go meow",
                "i go meow",
                "eukaryotes without cell walls",
            ],
            "language": ["en"] * 5,
            "language_confidence": [0.95] * 5,
            "columns_that_do_not_matter": ["processed_features", "status", "and", "other", "cols"],
        }
    )

    expected_df = pd.DataFrame(
        data={
            "object_id": ["id_1", "id_2"],
            "class": ["doubly_filled_in_class_user_error", "animal"],
            "text": ["furry best friend", "i go meow"],
            "language": ["en"] * 2,
            "language_confidence": [0.95] * 2,
        },
        index=[0, 3],
    )

    actual_df = pull_label_sheet.extract_labelled_examples_with_no_feature(input_df)
    pd.testing.assert_frame_equal(actual_df, expected_df)
