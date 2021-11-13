"""Language detection tagging."""
from typing import List, Tuple

import logging

import fasttext
import pandas as pd

from phoenix.common.artifacts import urls


def execute(series_of_text: pd.Series):
    """From a series of text return the series of detect languages."""
    ft = fasttext.load_model(urls.get_local_models() + "lid.176.bin")
    res = series_of_text.apply(lambda x: detect(x, model=ft))
    return res


def detect(message: str, model: fasttext.FastText) -> pd.Series:
    """Detect for a message.

    Arguments:
        message (str): message for which you want the language detected
        model (fasttext.FastText) pretrained model for language detection

    Returns:
        pd.Series with dtypes:
            0: string,
            1: float,
    """
    codes, confidences = model.predict(message, k=5) if not pd.isna(message) else (["en"], [0.0])
    code, confidence = get_top_whitelisted_language(codes, confidences)
    return pd.Series([code, confidence])


def get_top_whitelisted_language(
    lang_list: List[str], conf_list: List[float]
) -> Tuple[str, float]:
    """Get the language detected with the most confidence, which is on the whitelist."""
    whitelist = ["en", "ar", "ku", "ckb"]
    clean_codes = [code.removeprefix("__label__") for code in lang_list]
    for i in range(len(clean_codes)):
        if clean_codes[i] in whitelist and conf_list[i] > 0.15:
            return clean_codes[i], conf_list[i]

    logging.info("Language not detected, Default return is ar_izi, 0.0")
    return "und", 0.0
