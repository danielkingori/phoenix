"""Tag tensions in objects."""

import pandas as pd

from phoenix.custom_models.tension_classifier.process_annotations import TENSIONS_COLUMNS_LIST


def tag_object_has_tension(df: pd.DataFrame) -> pd.DataFrame:
    """Create the has_tension column which checks if any tensions are predicted in an object."""
    if not set(TENSIONS_COLUMNS_LIST).issubset(df.columns):
        raise ValueError(
            "DataFrame doesn't have all the tensions as columns."
            f"Should have {TENSIONS_COLUMNS_LIST}, has {df.columns}"
        )
    df["has_tension"] = df[TENSIONS_COLUMNS_LIST].isin([True]).any(1)

    return df


def normalise_tension_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the tension cols."""
    for tension_col in TENSIONS_COLUMNS_LIST:
        if tension_col not in df.columns:
            df[tension_col] = False
    df[TENSIONS_COLUMNS_LIST] = df[TENSIONS_COLUMNS_LIST].astype(bool)
    return df


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the tensions."""
    df = normalise_tension_cols(df)
    return tag_object_has_tension(df)
