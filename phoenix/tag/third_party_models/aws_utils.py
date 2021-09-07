"""AWS Utils."""
import functools

import pandas as pd


SENTIMENT_MAX_BYTES = 5000


def text_bytes_truncate(series_of_text, max_bytes: int = SENTIMENT_MAX_BYTES) -> pd.Series:
    """Return the series of text truncated by bytes."""
    fn = functools.partial(utf8_byte_truncate, max_bytes)
    return series_of_text.apply(fn).str.decode("utf8")


# Code taken from
# https://stackoverflow.com/questions/13727977/truncating-string-to-byte-length-in-python
def utf8_lead_byte(b):
    """A UTF-8 intermediate byte starts with the bits 10xxxxxx."""
    return (b & 0xC0) != 0x80


def utf8_byte_truncate(max_bytes, text):
    """Trucate UTF8 string by max bytes.

    If text[max_bytes] is not a lead byte, back up until a lead byte is
    found and truncate before that character.
    """
    utf8 = text.encode("utf8")
    if len(utf8) <= max_bytes:
        return utf8
    i = max_bytes
    while i > 0 and not utf8_lead_byte(utf8[i]):
        i -= 1
    b_str = utf8[:i]
    return b_str
