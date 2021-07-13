"""Language detection tagging."""
from typing import Tuple

import logging

import dask.dataframe as dd
import pandas as pd
from polyglot.detect import Detector


def execute(series_of_text: pd.Series):
    """From a series of text return the series of detect languages."""
    ddf = dd.from_pandas(
        series_of_text, npartitions=30
    )  # TODO: Should have npartitions configured in envirnment
    return ddf.apply(detect, meta={0: str, 1: int}).compute()


def detect(message: str) -> pd.Series:
    """Detect for a message.

    Arguments:
        text message

    Returns:
        pd.Series with dtypes:
            0: string,
            1: int,
    """
    code, confidence = detect_lang_code(message)
    if code not in ["en", "ar"]:
        code = "ar_izi"
    return pd.Series([code, confidence])


def detect_lang_code(message: str) -> Tuple[str, float]:
    """This is for testing.

    Not able to mock out the polyglot detector.
    """
    try:
        ld = Detector(message)
        return ld.language.code, ld.language.confidence
    # TODO: errors should be added as a new language type
    # This will then clarify which texts can't be detected on
    except:  # noqa[E722]
        logging.info(f"Info: An exception the detection occured for message {message}")
        return "en", 0.0
