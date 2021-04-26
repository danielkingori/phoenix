"""Language detection tagging."""
import pandas as pd
import textblob


def execute(ser: pd.Series) -> pd.Series:
    """From a series of messages return the series of detect languages."""
    return ser.apply(detect)


def detect(message: str):
    """Detect Language."""
    tb = textblob.TextBlob(message)
    return tb.detect_language()
