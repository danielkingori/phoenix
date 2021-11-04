"""Pull the labeling sheet with human-annotated labels."""

import pandas as pd

from phoenix.tag.labeling.generate_label_sheet import EXPECTED_COLUMNS_OBJECT_LABELING_SHEET


def is_valid_labeling_sheet(df: pd.DataFrame) -> bool:
    """Check if DataFrame is a valid labeling sheet.

    Args:
        df (pd.DataFrame): df to be checked if it's a valid labeling sheet.
    """
    for col_name in EXPECTED_COLUMNS_OBJECT_LABELING_SHEET:
        if col_name not in df.columns:
            return False

    return True
