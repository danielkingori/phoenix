"""Language detection tagging."""
from polyglot.detect import Detector
import pandas as pd


def execute(ser: pd.Series):
    """From a series of messages return the series of detect languages."""
    return ser.apply(detect)


def detect(message: str):
    """Detect for a message."""
    ld = Detector(message)
    return pd.Series([ld.language.code, ld.language.confidence])
