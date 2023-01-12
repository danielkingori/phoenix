"""Module containing backporting functions to go from SFLM to legacy sfm configurations."""
import pandas as pd


def sflm_to_sfm(sflm_df: pd.DataFrame) -> pd.DataFrame:
    """Transform Single Feature to Label Mapping config to legacy Single Feature Mapping."""
    sflm_df = sflm_df[sflm_df["status"] == "active"]
    if "use_processed_features" in sflm_df:
        sflm_df["use_processed_features"] = (
            sflm_df["use_processed_features"].str.lower().isin(["true", "t"])
        )
        sfm_df = sflm_df[
            ["unprocessed_features", "processed_features", "class", "use_processed_features"]
        ]
    else:
        sfm_df = sflm_df[["processed_features", "class"]]
    sfm_df = sfm_df.drop_duplicates()
    sfm_df = sfm_df.rename(columns={"processed_features": "features", "class": "topic"})
    return sfm_df
