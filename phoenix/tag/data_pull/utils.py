"""Utils for pulling data."""
import re

import pandas as pd


def to_type(column_name: str, astype, df: pd.DataFrame):
    """Name column string."""
    df[column_name] = df[column_name].astype(astype)
    return df


def words_to_snake(name: str):
    """Map Words with spaces to snake case."""
    return re.sub(r"\W+", "_", name.lower())
