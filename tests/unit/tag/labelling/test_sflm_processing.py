"""Unit tests for the sflm_processing module."""
import mock
import pandas as pd

from phoenix.tag.labelling import sflm_processing
from phoenix.tag.text_features_analyser import TextFeaturesAnalyser


def test_convert_to_bool():
    """Tests the convert_to_bool function."""
    input_df = pd.DataFrame(
        data={
            "some_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "maybe_bool_col": [True, False, "True", "False", "TRUE", "FALSE", "truuue", "false"],
        }
    )

    expected_df = pd.DataFrame(
        data={
            "some_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "maybe_bool_col": [True, False, True, False, True, False, True, False],
        }
    )

    actual_df = input_df.copy()
    actual_df["maybe_bool_col"] = actual_df["maybe_bool_col"].apply(
        sflm_processing.convert_to_bool
    )
    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_normalise_sflm_from_sheets():
    """Test normalise_sflm_from_sheets."""
    input_df = pd.DataFrame(
        {
            "class": ["c1", 2],
            "unprocessed_features": ["unpf1", 100],
            "processed_features": ["pf1", 2],
            "int_col": [1, 2],
            "language_confidence": ["", 0.342],
        }
    )
    expected_df = pd.DataFrame(
        {
            "class": ["c1", "2"],
            "unprocessed_features": ["unpf1", "100"],
            "processed_features": ["pf1", "2"],
            "int_col": [1, 2],
            "language_confidence": [0.0, 0.342],
        }
    )
    result = sflm_processing.normalise_sflm_from_sheets(input_df)
    pd.testing.assert_frame_equal(result, expected_df)


def test_update_changed_processed_features():
    """Test overwrite_changed_rows function."""
    input_old_df = pd.DataFrame(
        {
            "class": ["dog", "cat", "insect", "dog", "cat"],
            "unprocessed_features": ["woofs", "meows", "buzzes", "howls", "purring"],
            "processed_features": ["woof", "meow", "buzz", "howl", "purring"],
            "another_column": ["old_1", "old_2", "old_3", "old_4", "old_5"],
        }
    )

    input_new_df = pd.DataFrame(
        {
            "class": ["dog", "cat", "insect", "dog", "cat", "insect"],
            "unprocessed_features": ["woofs", "meows", "buzzes", "howls", "purring", "stings"],
            "processed_features": ["woof", "meo", "buz", "howl", "pur", "sting"],
            "another_column": ["new_1", "new_2", "new_3", "new_4", "new_5", "new_6"],
        }
    )

    expected_df = pd.DataFrame(
        {
            "class": ["dog", "cat", "insect", "dog", "cat", "insect"],
            "unprocessed_features": ["woofs", "meows", "buzzes", "howls", "purring", "stings"],
            "processed_features": ["woof", "meo", "buz", "howl", "pur", "sting"],
            "another_column": ["old_1", "new_2", "new_3", "old_4", "new_5", "new_6"],
        }
    )

    actual_df = sflm_processing.update_changed_processed_features(input_new_df, input_old_df)
    pd.testing.assert_frame_equal(actual_df, expected_df)


@mock.patch("phoenix.tag.labelling.sflm_processing.text_features_analyser")
def test_reprocess_sflm(mock_tfa_module):
    """Test reprocess_sflm replaces any rows that have a changed processed_features output.

    It changes the use_processed_features to False when the processed feature changes.
    It only updates the status to analyst_action needed when the input row's status was active.
    It removes any duplicates in the input sflm
    """
    mock_tfa = mock.MagicMock(TextFeaturesAnalyser)
    mock_tfa_module.create.return_value = mock_tfa

    mock_tfa.features.return_value = pd.Series(
        data=[["woof"], ["meo", "soft"], ["buz"], ["howl"], ["pur"]]
    )

    input_sflm_df = pd.DataFrame(
        {
            "class": ["dog", "cat", "insect", "dog", "cat", "dog"],
            "unprocessed_features": [
                "woofs",
                "meows softly",
                "buzzes",
                "howls",
                "purring",
                "woofs",
            ],
            "processed_features": ["woof", "meow soft", "buzz", "howl", "purring", "woof"],
            "use_processed_features": [True, True, True, True, False, True],
            "status": ["active", "active", "active", "active", "deleted", "active"],
        }
    )

    expected_df = pd.DataFrame(
        {
            "class": ["dog", "cat", "insect", "dog", "cat"],
            "unprocessed_features": ["woofs", "meows softly", "buzzes", "howls", "purring"],
            "processed_features": ["woof", "meo soft", "buz", "howl", "pur"],
            "use_processed_features": [True, False, False, True, False],
            "status": [
                "active",
                "analyst_action_needed",
                "analyst_action_needed",
                "active",
                "deleted",
            ],
        }
    )

    actual_df = sflm_processing.reprocess_sflm(input_sflm_df)

    mock_tfa_module.create.assert_called_with(use_ngrams=False)

    mock_tfa.features.assert_called_once()
    mock_tfa.features.assert_called_with(mock.ANY, "unprocessed_features")
    pd.testing.assert_frame_equal(actual_df, expected_df)
