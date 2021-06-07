"""Test language."""
import mock
import pandas as pd
import pytest

from phoenix.tag import language


@mock.patch("phoenix.tag.language.detect")
def test_execute(m_detect):
    """Test that the execute of language."""
    m1 = "m1"
    m2 = "m2"
    m3 = "m3"
    ser = pd.Series(
        [
            m1,
            m2,
            m3,
        ],
        name="message",
    )
    m_detect.return_value = pd.Series(["en", 1])
    r_value = language.execute(ser)
    calls = [
        mock.call(m1),
        mock.call(m2),
        mock.call(m3),
    ]
    assert any([elem in calls for elem in m_detect.mock_calls])
    pd.testing.assert_frame_equal(r_value, pd.DataFrame({0: ["en"] * 3, 1: [1] * 3}))


@pytest.mark.parametrize("mock_lang,expected_lang", [("ar", "ar"), ("en", "en"), ("ci", "ar_izi")])
@mock.patch("phoenix.tag.language.detect_lang_code")
def test_detect(m_detect_lang_code, mock_lang, expected_lang):
    """Test that the execute of language."""
    m1 = "m1"
    confidence = 1.0
    m_detect_lang_code.return_value = (mock_lang, confidence)
    r_value = language.detect(m1)
    m_detect_lang_code.assert_called_once_with(m1)
    pd.testing.assert_series_equal(r_value, pd.Series([expected_lang, confidence]))