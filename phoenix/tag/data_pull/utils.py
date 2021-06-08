"""Utils for pulling data."""
import re

import pandas as pd


def to_type(column_name: str, astype, df: pd.DataFrame):
    """Name column string."""
    df[column_name] = df[column_name].astype(astype)
    return df


def snake_names(name: str):
    """Map column names."""
    return re.sub(r"\W+", "_", name.lower())
