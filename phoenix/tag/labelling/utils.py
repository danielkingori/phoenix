"""Utility functions for the labelling submodule."""
import pandas as pd

from phoenix.tag.labelling.generate_label_sheet import EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET


def get_account_object_type(object_type: str) -> str:
    """Get an account_object_type from the object_type."""
    object_type_mapping = {
        "facebook_posts": "facebook_pages",
        "tweets": "twitter_handles",
        "youtube_videos": "youtube_channels",
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
