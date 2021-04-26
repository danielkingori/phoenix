"""Language detection tagging."""
import cld3
import pandas as pd


def execute(ser: pd.Series):
    """From a series of messages return the series of detect languages."""
    return ser.apply(detect)


def detect(message: str):
    """Detect for a message."""
    ld = cld3.get_language(message)
    return pd.Series([ld.language, ld.probability])
