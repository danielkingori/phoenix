"""Unit tests for custom_models utils."""

import pandas as pd

from phoenix.custom_models import utils


def test_explode_str():
    expected_df = pd.DataFrame(
        {
            "a": ["hello", "world", "foo", "bar", "baz"],
            "b": ["string_1", "string_1", "string_2", "string_2", "string_2"],
        },
        index=[0, 0, 1, 1, 1],
    )
    input_df = pd.DataFrame(
        {"a": ["hello, world", "foo, bar, baz"], "b": ["string_1", "string_2"]}
    )
    output_df = utils.explode_str(input_df, "a", ",")

    pd.testing.assert_frame_equal(expected_df, output_df)
