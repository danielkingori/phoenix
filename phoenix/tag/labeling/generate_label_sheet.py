"""Generate labeling sheet for human labelers."""

import pandas as pd


EXPECTED_COLUMNS_LABELING_SHEET = [
    "object_id",
    "object_type",
    "object_url",
    "created_at",
    "object_user_url",
    "matched_labels",
    "matched_features",
    "text",
    "label_1",
    "label_1_features",
    "label_2",
    "label_2_features",
    "label_3",
    "label_3_features",
    "label_4",
    "label_4_features",
    "label_5",
    "label_5_features",
]


def create_new_labeling_sheet_df(for_tag_df: pd.DataFrame) -> pd.DataFrame:
    """Create a new labeling sheet using the for_tagging pulled data.

    Args:
        for_tag_df (pd.DataFrame): dataframe with data from the tag/data_pull scripts.
    """
    for col in EXPECTED_COLUMNS_LABELING_SHEET:
        if col not in for_tag_df.columns:
            for_tag_df[col] = None

    return for_tag_df[EXPECTED_COLUMNS_LABELING_SHEET]
