"""Test language."""
import fasttext
import mock
import pandas as pd
import pytest

from phoenix.tag import language


@mock.patch("phoenix.tag.language.fasttext.load_model")
@mock.patch("phoenix.tag.language.detect")
def test_execute(m_detect, m_fasttext_loader):
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
    m_loaded_model = mock.MagicMock()
    m_fasttext_loader.return_value = m_loaded_model
    m_detect.return_value = pd.Series(["en", 1])
    r_value = language.execute(ser)
    calls = [
        mock.call(m1, model=m_loaded_model),
        mock.call(m2, model=m_loaded_model),
        mock.call(m3, model=m_loaded_model),
    ]
    assert any([elem in calls for elem in m_detect.mock_calls])
    pd.testing.assert_frame_equal(r_value, pd.DataFrame({0: ["en"] * 3, 1: [1] * 3}))


@pytest.mark.parametrize(
    "mock_lang,expected_lang",
    [("__label__ar", "ar"), ("__label__en", "en"), ("__label__ku", "ku"), ("__label__ckb", "ckb")],
)
def test_detect(mock_lang, expected_lang):
    """Test that the execute of language."""
    m1 = "m1"
    confidence = 0.4
    m_model = mock.MagicMock(spec=fasttext.FastText._FastText)

    m_model.predict.return_value = (["__label__unlisted_lang", mock_lang], [0.6, confidence])
    r_value = language.detect(m1, m_model)
    m_model.predict.assert_called_once_with(m1, k=5)
    pd.testing.assert_series_equal(r_value, pd.Series([expected_lang, confidence]))


def test_detect_default():
    """Test that the execute of language."""
    m1 = "m1"
    m_model = mock.MagicMock(spec=fasttext.FastText._FastText)

    m_model.predict.return_value = (["unlisted_lang_1", "unlisted_lang_2"], [0.6, 0.4])
    r_value = language.detect(m1, m_model)
    m_model.predict.assert_called_once_with(m1, k=5)
    pd.testing.assert_series_equal(r_value, pd.Series(["und", 0.0]))
