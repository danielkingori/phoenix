"""Language detection tagging."""
from typing import List, Optional, Tuple

import logging
import os

import fasttext
import pandas as pd
import tentaclio

from phoenix.common.artifacts import urls, utils


def execute(series_of_text: pd.Series):
    """From a series of text return the series of detect languages."""
    ft = load_model("lid.176.bin")
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
    codes, confidences = (
        model.predict(message.replace("\n", " "), k=5) if not pd.isna(message) else (["en"], [0.0])
    )
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

    logging.info("Language not detected, Default return is und, 0.0")
    return "und", 0.0


def load_model(model_name: str, external_download_link: Optional[str] = None) -> fasttext.FastText:
    """Load a fasttext_model named model_name from local. Lazily downloads if not exists.

    Args:
        model_name (str): file_name of the model (with extension): eg "lid.176.bin"
        external_download_link (Optional[str]): where to download the model if it does not
            exist. This needs to be a valid tentaclio path eg "s3://my_bucket/lid.176.bin".
            Defaults to FB AI' public fasttext repository.

    Returns:
        fasttext.FastText: A language detection model.
    """
    if not external_download_link:
        external_download_link = (
            "https://dl.fbaipublicfiles.com/fasttext/supervised-models/" + model_name
        )

    if not os.path.isfile(urls.get_local_models() + model_name):
        with tentaclio.open(external_download_link, "rb") as fh:
            model = fh.read()

        local_model_filepath = f"file://{urls.get_local_models()}{model_name}"
        utils.create_folders_if_needed(local_model_filepath)

        with tentaclio.open(local_model_filepath, "wb") as write_handle:
            write_handle.write(model)

    return fasttext.load_model(urls.get_local_models() + model_name)
