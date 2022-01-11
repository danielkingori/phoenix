"""Module to process the Single Feature to Label Mapping."""
from typing import Union

import pandas as pd


DEDUPLICATIONS_COLUMNS = ["class", "unprocessed_features", "processed_features"]


def convert_to_bool(input_val: Union[str, bool]) -> bool:
    """Converts string representations of booleans into bools. Any non-false string is True."""
    if type(input_val) == bool:
        return input_val

    if input_val.lower() == "false":
        return False
    else:
        return True


def normalise_sflm_from_sheets(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the sflm dataframe from sheets."""
    for col in DEDUPLICATIONS_COLUMNS:
        df[col] = df[col].astype(str)

    if "object_id" in df.columns:
        df["object_id"] = df["object_id"].astype(str)

    return df
