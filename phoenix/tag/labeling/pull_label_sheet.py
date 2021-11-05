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


def extract_features_to_label_mapping_objects(df: pd.DataFrame) -> pd.DataFrame:
    """Extract features to class mappings."""
    if not is_valid_labeling_sheet(df):
        ValueError(
            f"Dataframe does not have correct columns to be an object labeling sheet. {df.columns}"
        )

    feature_to_label_df = wide_to_long_labels_features(df)

    return feature_to_label_df


def wide_to_long_labels_features(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a long dataframe for labels and their features.

    Note: this is pretty brittle, refactor soon
    """
    df_labels = pd.DataFrame(columns=["object_id", "class", "unprocessed_features"])

    for i in range(1, 6):
        COLS = df.filter(like=f"label_{i}").columns
        COLS = COLS.append(df.filter(like="object_id").columns)
        temp_df = df[COLS].copy()
        temp_df = temp_df.rename(
            {f"label_{i}": "class", f"label_{i}_features": "unprocessed_features"}, axis=1
        )
        # replace empty strings with NaN to be able to use dropna
        temp_df[["class", "unprocessed_features"]] = temp_df[
            ["class", "unprocessed_features"]
        ].replace(r"^\s*$", np.nan, regex=True)
        temp_df = temp_df.dropna(subset=["class", "unprocessed_features"], how="all")

        df_labels = df_labels.append(temp_df)

    df_labels["unprocessed_features"] = df_labels["unprocessed_features"].str.split(",")
    df_labels = df_labels.explode("unprocessed_features").reset_index(drop=True)
    df_labels["unprocessed_features"] = df_labels["unprocessed_features"].fillna("")

    return df_labels
