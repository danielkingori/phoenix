"""Language detection tagging."""
import functools

import pandas as pd
from google.cloud import translate_v2 as translate


def execute(ser: pd.Series) -> pd.Series:
    """From a series of messages return the series of detect languages."""
    translate_client = translate.Client()
    curried_fn = functools.partial(translate_value, translate_client)
    return ser.apply(curried_fn)


def translate_value(client, message: str):
    """Translate."""
    result = client.translate(message, target_language="ar")
    return pd.Series([result["translatedText"], result["detectedSourceLanguage"]])
