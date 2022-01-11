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

    # Having problems with `language_confidence` being an empty string
    # this normalises it to 0
    if "language_confidence" in df.columns:
        df["language_confidence"] = (
            df["language_confidence"]
            .apply(lambda x: x.strip() if isinstance(x, str) else x)
            .replace("", 0)
        )

    return df
