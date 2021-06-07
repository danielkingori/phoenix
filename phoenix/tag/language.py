"""Language detection tagging."""
from typing import Tuple

import dask.dataframe as dd
import pandas as pd
from polyglot.detect import Detector


def execute(ser: pd.Series):
    """From a series of messages return the series of detect languages."""
    ddf = dd.from_pandas(ser, npartitions=30)  # Should have npartitions configured in envirnment
    return ddf.apply(detect, meta={0: str, 1: int}).compute()


def detect(message: str):
    """Detect for a message."""
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
    # TODO: fix this
    except:  # noqa[E722]
        print("An exception occurred")
        return "en", 0.0
