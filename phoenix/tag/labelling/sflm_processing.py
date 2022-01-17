"""Module to process the Single Feature to Label Mapping."""
from typing import Union

import pandas as pd

from phoenix.tag import text_features_analyser
from phoenix.tag.labelling import utils


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


def update_changed_processed_features(new_df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
    """Overwrites rows in old_df with new_df if, and only if the processed feature has changed.

    Args:
        old_df (pd.DataFrame): dataframe containing at least the cols ["class",
            "unprocessed_features", "processed_features"]
        new_df (pd.DataFrame): dataframe containing at least the cols ["class",
        "unprocessed_features", "processed_features"]

    Returns:
        pd.DataFrame: df where rows that are duplicates along all three columns come from
            old_df, any that aren't duplicates come from the new_df
    """
    new_processed_features_df = utils.filter_out_duplicates(
        old_df,
        new_df,
        ["class", "unprocessed_features", "processed_features"],
    )
    no_change_df = utils.filter_out_duplicates(
        new_processed_features_df, old_df, ["class", "unprocessed_features"]
    )
    returnable_df = no_change_df.append(new_processed_features_df).sort_index()
    return returnable_df


def reprocess_sflm(sflm_df: pd.DataFrame) -> pd.DataFrame:
    """Reprocess the processed_features in the sflm dataframe."""
    sflm_df = sflm_df.drop_duplicates(subset=["class", "unprocessed_features"])
    reprocessed_labelled_objects = sflm_df.copy()
    reprocessed_labelled_objects["use_processed_features"] = False
    # only change the status to analyst_action_needed if it was previously active
    mask = reprocessed_labelled_objects["status"] == "active"
    reprocessed_labelled_objects.loc[mask, "status"] = "analyst_action_needed"

    tfa = text_features_analyser.create(use_ngrams=False)
    reprocessed_labelled_objects["processed_features"] = tfa.features(
        reprocessed_labelled_objects, "unprocessed_features"
    )
    # This defensive cast through mapping each list of strings to a string should never be
    # needed over just a str(str_list), but just in case, we've gone defensive.
    reprocessed_labelled_objects["processed_features"] = [
        " ".join(map(str, str_list))
        for str_list in reprocessed_labelled_objects["processed_features"]
    ]

    returnable_df = update_changed_processed_features(reprocessed_labelled_objects, sflm_df)

    return returnable_df
