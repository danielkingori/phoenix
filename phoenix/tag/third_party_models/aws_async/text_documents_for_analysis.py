"""Text documents for analysis."""
import os

import numpy as np
import pandas as pd
import tentaclio


def persist_text_series(url: str, text_series: pd.Series):
    """Persist text series for AWS Async job."""
    if url.startswith("file:"):
        plain_url = url[len("file:") :]
        os.makedirs(os.path.dirname(plain_url), exist_ok=True)

    with tentaclio.open(url, "w") as file_io:
        np.savetxt(file_io, text_series.values, fmt="%s")

    return url


def read_text_series(url: str) -> pd.Series:
    """Read text series from AWS Async job."""
    with tentaclio.open(url, "r") as file_io:
        np_text = np.loadtxt(file_io, dtype=str)

    return pd.Series(np_text)
