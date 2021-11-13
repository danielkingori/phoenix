"""Integration test for language detection."""
import pandas as pd
import pytest

from phoenix.tag import language


@pytest.mark.parametrize(
    "sentence,expected_lang",
    [
        ("المعادلة النفطية: جيش ، شعب ، بنزين", "ar"),
        ("Hello and good morning", "en"),
        ("Ser çavan, gelekî kêfxweş bûm", "ku"),
        ("من باشم، سوپاست دەکەم، ئەی تۆ؟", "ckb"),
        ("shi jdid, 7ala2  beit w rayi7 3al she5el w reji3 da7ir ma3 lshabeb w inte?", "und"),
    ],
)
def test_fasttext_pretrained_model(sentence, expected_lang):
    """Test that the fasttext pretrained model does correct language detection."""

    series = pd.Series(data=[sentence])

    actual_result = language.execute(series)

    # assert that the language (first index) of the first row seen is the expected language.
    assert actual_result[0][0] == expected_lang
