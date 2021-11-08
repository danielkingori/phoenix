"""Pull the labeling sheet with human-annotated labels."""
from typing import Tuple

import numpy as np
import pandas as pd

from phoenix.tag import language, text_features_analyser
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


def extract_features_to_label_mapping_objects(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract features to class mappings.

    Args:
        df (pd.DataFrame): filled in manually labeled sheet as defined by
            EXPECTED_COLUMNS_LABELING_SHEET

    Returns:
        df (pd.DataFrame): Feature to label mapping dataframe
        df (pd.DataFrame): objects with labels with no features dataframe

    Runs the language detection from tag.language on the "text" column, and uses that language
    to run the feature processing pipeline without splitting text into ngrams.
    """
    if not is_valid_labeling_sheet(df):
        ValueError(
            f"Dataframe does not have correct columns to be an object labeling sheet. {df.columns}"
        )

    feature_to_label_df = wide_to_long_labels_features(df)
    feature_to_label_df = feature_to_label_df.merge(
        df[["object_id", "text"]], how="left", on="object_id"
    )
    feature_to_label_df[["language", "language_confidence"]] = language.execute(
        feature_to_label_df["text"]
    )

    tfa = text_features_analyser.create(use_ngrams=False)
    feature_to_label_df["processed_features"] = tfa.features(
        feature_to_label_df, "unprocessed_features"
    )

    # This defensive cast through mapping each list of strings to a string should never be
    # needed over just a str(str_list), but just in case, we've gone defensive.
    feature_to_label_df["processed_features"] = [
        " ".join(map(str, str_list)) for str_list in feature_to_label_df["processed_features"]
    ]

    feature_to_label_df["use_processed_features"] = True
    feature_to_label_df["status"] = "active"

    feature_to_label_df["unprocessed_features"] = feature_to_label_df[
        "unprocessed_features"
    ].replace(r"^\s*$", np.nan, regex=True)
    label_with_no_feature_df = feature_to_label_df[
        feature_to_label_df["unprocessed_features"].isna()
    ].fillna("")
    feature_to_label_df = feature_to_label_df.drop("text", axis=1)
    feature_to_label_df = feature_to_label_df.dropna(subset=["unprocessed_features"])
    feature_to_label_df = feature_to_label_df.fillna("")

    return feature_to_label_df, label_with_no_feature_df


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
