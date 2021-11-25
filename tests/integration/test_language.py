"""Integration test for language detection."""
import os

import fasttext
import mock
import pandas as pd
import pytest

from phoenix.tag import language


@mock.patch("phoenix.tag.language.urls.get_local_models")
def test_load_model(m_local_models, tmp_path):
    """Test that load_model can download models when the model is not present on local.

    Uses the smaller model lid.176.ftz instead of the lid.176.bin which is in production as it
    is ~100x smaller.
    """
    m_local_models.return_value = str(tmp_path)
    file_name = "lid.176.ftz"
    assert not os.path.isfile(str(tmp_path) + file_name)
    ft_model = language.load_model(file_name)
    assert type(ft_model) == fasttext.FastText._FastText
    assert os.path.isfile(str(tmp_path) + file_name)


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
