"""Language detection tagging."""
import pandas as pd
from polyglot.detect import Detector


def execute(ser: pd.Series):
    """From a series of messages return the series of detect languages."""
    return ser.apply(detect)


def detect(message: str):
    """Detect for a message."""
    ld = Detector(message)
    code = ld.language.code
    if code not in ["en", "ar"]:
        code = "ar_izi"
    return pd.Series([code, ld.language.confidence])
