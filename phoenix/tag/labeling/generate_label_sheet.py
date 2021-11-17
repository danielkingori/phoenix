"""Generate labeling sheet for human labelers."""
from typing import Tuple

import pandas as pd
from sklearn.model_selection import train_test_split


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
    "account_label_1",
    "account_label_2",
    "account_label_3",
]


def create_new_labeling_sheet_df(for_tag_df: pd.DataFrame) -> pd.DataFrame:
    """Create a new labeling sheet using the for_tagging pulled data.

    Args:
        for_tag_df (pd.DataFrame): dataframe with data from the tag/data_pull scripts.
    """
    for col in EXPECTED_COLUMNS_LABELING_SHEET:
        if col not in for_tag_df.columns:
            for_tag_df[col] = None

    for_tag_df["created_at"] = for_tag_df["created_at"].apply(
        lambda x: pd.to_datetime(x).strftime("%Y-%m-%d %H:%M") if x else None
    )

    return for_tag_df[EXPECTED_COLUMNS_LABELING_SHEET]


def get_goal_number_rows(
    df: pd.DataFrame, stratify_col: str, n: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Stratified split a dataframe based on a column to get a goal number of rows.

    Args:
        df (pd.DataFrame): dataframe to split
        stratify_col (str): which column do you want to use to ensure a representative number
            when splitting
        n (int): Target number to get
    Returns:
        pd.DataFrame: excluded rows
        pd.DataFrame: included rows
    """
    if len(df) > n:
        test_size = n / len(df)
        # remove rows when the stratified column only has one object; needed for train_test_split
        no_non_duplicates_df = df[df[stratify_col].duplicated(keep=False)]
        excluded_df, df = train_test_split(
            no_non_duplicates_df,
            test_size=test_size,
            stratify=no_non_duplicates_df[[stratify_col]],
            random_state=42,
        )
        excluded_df.append(df[~df[stratify_col].duplicated(keep=False)])
    else:
        excluded_df = pd.DataFrame(columns=df.columns)

    return excluded_df, df
