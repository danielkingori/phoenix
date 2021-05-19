"""Language detection tagging."""
from typing import Tuple

import pandas as pd
from polyglot.detect import Detector


def execute(ser: pd.Series):
    """From a series of messages return the series of detect languages."""
    return ser.apply(detect)


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
    ld = Detector(message)
    return ld.language.code, ld.language.confidence
