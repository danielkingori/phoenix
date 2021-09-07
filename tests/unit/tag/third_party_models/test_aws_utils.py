"""Test AWS utils."""
import pandas as pd
import pytest

from phoenix.tag.third_party_models import aws_utils


@pytest.mark.parametrize(
    "max_bytes,series_of_text,expected_result",
    [
        (
            1,
            pd.Series(["12", "1"]),
            pd.Series(["1", "1"]),
        ),
        (
            8,
            pd.Series(["12345678910", "12345"]),
            pd.Series(["12345678", "12345"]),
        ),
    ],
)
def test_text_bytes_truncate(max_bytes, series_of_text, expected_result):
    """Test text_bytes_truncate."""
    result = aws_utils.text_bytes_truncate(series_of_text, max_bytes)
    pd.testing.assert_series_equal(result, expected_result)
