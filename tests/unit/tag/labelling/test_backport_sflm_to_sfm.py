"""Unit tests for backport_sflm_to_sfm."""
import pandas as pd

from phoenix.tag.labelling import backport_sflm_to_sfm


def test_sflm_to_sfm():
    input_df = pd.DataFrame(
        data={
            "object_id": ["id_1", "id_2", "id_3", "id_4"],
            "class": ["dog", "cat", "dog", "insect"],
            "unprocessed_features": ["woofs", "meows", "barks", "buzzes"],
            "language": ["en", "en", "en", "en"],
            "language_confidence": [0.95, 0.95, 0.95, 0.95],
            "processed_features": ["woof", "meow", "bark", "buzz"],
            "use_processed_features": [True, False, True, False],
            "status": ["active", "deleted", "active", "active"],
        }
    )

    expected_df = pd.DataFrame(
        data={"features": ["woofs", "barks", "buzzes"], "topic": ["dog", "dog", "insect"]}
    )

    actual_df = backport_sflm_to_sfm.sflm_to_sfm(input_df)

    pd.testing.assert_frame_equal(actual_df.reset_index(drop=True), expected_df)
