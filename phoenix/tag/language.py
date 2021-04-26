"""Language detection tagging."""
import pandas as pd
from guess_language import guess_language


def execute(ser: pd.Series) -> pd.Series:
    """From a series of messages return the series of detect languages."""
    return ser.apply(detect)


def detect(message):
    """Detect on the message."""
    return guess_language(message)
