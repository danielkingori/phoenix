"""Test Normalise of data for tagging."""
import pandas as pd

from phoenix.tag import normalise


def test_is_unofficial_retweet():
    """Test if is_unofficial_retweet."""
    input_df = pd.DataFrame({"clean_text": ["RT @", "R @", "RT message", "messages RT"]})
    result = normalise.is_unofficial_retweet(input_df["clean_text"])
    pd.testing.assert_series_equal(
        result, pd.Series([True, False, True, False], name="clean_text")
    )


def test_is_retweet():
    """Test if is_reweet."""
    input_df = pd.DataFrame(
        {
            "is_unofficial_retweet": [True, False, True, False],
            "retweeted": [True, False, None, None],
        }
    )
    result = normalise.is_retweet(input_df)
    pd.testing.assert_series_equal(result, pd.Series([True, False, True, False]))
