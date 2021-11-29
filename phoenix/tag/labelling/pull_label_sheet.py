"""Pull the labelling sheet with human-annotated labels."""
from typing import Tuple

import numpy as np
import pandas as pd

from phoenix.tag import language, text_features_analyser
from phoenix.tag.labelling.generate_label_sheet import (
    EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET,
    EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET,
)
from phoenix.tag.labelling.utils import is_valid_account_labelling_sheet


def is_valid_labelling_sheet(df: pd.DataFrame) -> bool:
    """Check if DataFrame is a valid labelling sheet.

    Args:
        df (pd.DataFrame): df to be checked if it's a valid labelling sheet.
    """
    for col_name in EXPECTED_COLUMNS_OBJECT_LABELLING_SHEET:
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
    if not is_valid_labelling_sheet(df):
        ValueError(
            f"Dataframe does not have correct columns to be an object labelling sheet.{df.columns}"
        )

    # The first row of the df is made up of notes to the users doing the manual labelling and does
    # not contain data.
    df = df.drop(0)

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

    feature_to_label_df["unprocessed_features"] = feature_to_label_df[
        "unprocessed_features"
    ].replace(r"^\s*$", np.nan, regex=True)
    label_with_no_feature_df = feature_to_label_df[
        feature_to_label_df["unprocessed_features"].isna()
    ].fillna("")
    label_with_no_feature_df = label_with_no_feature_df[
        [
            "object_id",
            "class",
            "text",
            "language",
            "language_confidence",
        ]
    ].drop_duplicates(subset=["object_id", "class"])
    feature_to_label_df = clean_feature_to_label_df(feature_to_label_df)

    return feature_to_label_df, label_with_no_feature_df


def clean_feature_to_label_df(feature_to_label_df: pd.DataFrame) -> pd.DataFrame:
    """Clean the feature_to_label_df after extracting features and deduplicate.

    Sets defaults for "use_processed_features" and "status"
    Deduplicates based on "class", "unprocessed_features", "processed_features"
    """
    feature_to_label_df["use_processed_features"] = False
    feature_to_label_df["status"] = "active"
    feature_to_label_df = feature_to_label_df.drop("text", axis=1)
    feature_to_label_df = feature_to_label_df.dropna(subset=["unprocessed_features"])
    feature_to_label_df = feature_to_label_df.fillna("")

    feature_to_label_df = feature_to_label_df.drop_duplicates(
        subset=["class", "unprocessed_features", "processed_features"]
    )
    return feature_to_label_df


def get_account_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Get account labels and the people who labeled it.

    Args:
        df (pd.DataFrame): account labelling dataframe.
    """
    if not is_valid_account_labelling_sheet(df):
        ValueError(
            f"Dataframe doesn't have correct cols to be an account labelling sheet. {df.columns}"
        )

    account_df = df[EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET]
    long_account_df = pd.wide_to_long(
        account_df,
        stubnames="account_label",
        i=["object_user_name", "object_user_url", "labelled_by"],
        j="position",
        suffix="_\\d+",
    )
    long_account_df = long_account_df.reset_index().drop("position", axis=1).replace("", np.nan)
    long_account_df = long_account_df.dropna(subset=["account_label"], how="any", axis=0)
    return long_account_df


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
    # also split on the arabic comma
    df_labels["unprocessed_features"] = df_labels["unprocessed_features"].str.split("\u060c")
    df_labels = df_labels.explode("unprocessed_features").reset_index(drop=True)
    df_labels["unprocessed_features"] = df_labels["unprocessed_features"].fillna("")
    df_labels["unprocessed_features"] = df_labels["unprocessed_features"].str.strip()
    df_labels["class"] = df_labels["class"].str.strip()
    df_labels["class"] = df_labels["class"].str.lower()

    return df_labels
