"""Unit test for tag_tension."""

import pandas as pd
import pytest

from phoenix.tag.tension import tag_tension


def test_tag_object_has_tension_incorrect_cols():
    input_df = pd.DataFrame(
        {
            "tension": [True, False, False, False],
            "tension_2": [False, False, False, True],
            "not_a_tension": [False, True, True, True],
        }
    )

    with pytest.raises(ValueError):
        _ = tag_tension.tag_object_has_tension(input_df)


def test_tag_object_has_tension():
    input_df = pd.DataFrame(
        {
            "is_economic_labour_tension": [True, False, False, False],
            "is_sectarian_tension": [False, False, False, True],
            "is_environmental_tension": [False] * 4,
            "is_political_tension": [False] * 4,
            "is_service_related_tension": [False] * 4,
            "is_community_insecurity_tension": [False] * 4,
            "is_geopolitics_tension": [False] * 4,
            "is_intercommunity_relations_tension": [False] * 4,
            "not_a_tension": [True] * 4,
        }
    )
    expected_series = pd.Series(name="has_tension", data=[True, False, False, True])

    output_df = tag_tension.tag_object_has_tension(input_df)

    pd.testing.assert_series_equal(output_df["has_tension"], expected_series)
