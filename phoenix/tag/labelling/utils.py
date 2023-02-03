"""Utility functions for the labelling submodule."""
from typing import List, Optional

import pandas as pd

from phoenix.tag.labelling.generate_label_sheet import EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET


def get_account_object_type(object_type: str) -> str:
    """Get an account_object_type from the object_type."""
    object_type_mapping = {
        "facebook_posts": "facebook_pages",
        "tweets": "twitter_handles",
        "youtube_videos": "youtube_channels",
        "youtube_comments": "youtube_channels",
    }
    account_object_type = object_type_mapping.get(object_type, "")
    if not account_object_type:
        raise KeyError(f"No account object type found for {object_type}")

    return account_object_type


def is_valid_account_labelling_sheet(df: pd.DataFrame) -> bool:
    """Check if a dataframe has the correct columns to be an account_labelling sheet.

    Args:
        df (pd.DataFrame): df to be checked if it's a valid labelling sheet.
    """
    for col_name in EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET:
        if col_name not in df.columns:
            return False

    return True


def filter_out_duplicates(
    filled_sheet_df: pd.DataFrame, data_df: pd.DataFrame, cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """Filter out objects that are already in the filled_sheet_df by one or more columns.

    Args:
        filled_sheet_df (pd.DataFrame): data that users have already labelled data
        data_df (pd.DataFrame): df you want filtered by the filled_sheet_df
        cols (Optional[List[str]]): columns for which to check duplication in both dataframes.
            Defaults to ["object_id"]

    Returns:
        pd.DataFrame: subset of data_df which does not have a duplicate in filled_sheet_df.
            Duplication checked on the `cols` parameter.
    """
    cols = ["object_id"] if cols is None else cols
    # Check that all the cols are in the filled_sheet, otherwise overwrite
    if cols and not all(elem in filled_sheet_df for elem in cols):
        return data_df

    merged_df = pd.merge(data_df, filled_sheet_df[cols], how="left", on=cols, indicator=True)
    merged_df = merged_df.loc[merged_df["_merge"] == "left_only"].drop("_merge", axis=1)
    return merged_df
