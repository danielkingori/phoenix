"""Utils for custom models."""

import numpy as np
import pandas as pd


def explode_str(df: pd.DataFrame, col: str, sep: str):
    """Explodes columns that have strings separated by a `sep` into new rows.

    Args:
        df: (pd.DataFrame) dataframe in which you want to explode a column
        col: (str) column name that you want to explode
        sep: (str) separator that signifies it needs to be exploded
    """
    s = df[col]
    i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
    df = df.iloc[i].assign(**{col: sep.join(s).split(sep)})
    df[col] = df[col].str.lstrip()
    return df
