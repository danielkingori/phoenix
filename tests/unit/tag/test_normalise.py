"""Test Normalise of data for tagging."""
import mock
import pandas as pd

from phoenix.tag import normalise


@mock.patch("phoenix.tag.normalise.is_retweet")
@mock.patch("phoenix.tag.normalise.is_unofficial_retweet")
@mock.patch("phoenix.tag.language.execute")
@mock.patch("phoenix.tag.normalise.clean_text")
def test_execute(m_clean_text, m_lang_execute, m_is_unofficial_retweet, m_is_retweet):
    """Test execute."""
    m_df = mock.MagicMock(pd.DataFrame)
    normalise.execute(m_df)
    m_clean_text.assert_called_once()
    m_lang_execute.assert_called_once()
    m_is_unofficial_retweet.assert_called_once()
    m_is_retweet.assert_called_once()


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
