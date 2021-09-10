"""Unit tests for process_annotations."""

import numpy as np
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


def test_get_tensions_with_enough_examples():
    input_df = pd.DataFrame(
        np.array(
            [
                [1] * 8,
                [1] * 4 + [0] * 4,
                [1] * 2 + [0] * 6,
            ]
        ),
        columns=process_annotations.TENSIONS_COLUMNS_LIST,
    )

    expected_list = [
        "is_economic_labour_tension",
        "is_sectarian_tension",
        "is_environmental_tension",
        "is_political_tension",
    ]
    expected_tuples_list = [
        ("is_economic_labour_tension", 3, True),
        ("is_sectarian_tension", 3, True),
        ("is_environmental_tension", 2, True),
        ("is_political_tension", 2, True),
        ("is_service_related_tension", 1, False),
        ("is_community_insecurity_tension", 1, False),
        ("is_geopolitics_tension", 1, False),
        ("is_intercommunity_relations_tension", 1, False),
    ]

    actual_list, actual_tuples_list = process_annotations.get_tensions_with_enough_examples(
        input_df, minimum_num_examples=2
    )

    assert expected_list == actual_list
    assert expected_tuples_list == actual_tuples_list
