"""Tag module."""
import pandas as pd

from phoenix.tag import language


def execute(given_df: pd.DataFrame, message_key: str = "message") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    df["clean_message"] = clean_message(df[message_key])
    df[["language", "confidence"]] = language.execute(df["clean_message"])
    return df


def clean_message(message_ser) -> pd.Series:
    """Clean message."""
    return message_ser.replace(to_replace=r"https?:\/\/.*[\r\n]*", value="", regex=True)


def language_distribution(normalised_df) -> pd.DataFrame:
    """Get a distribution of languages."""
    lang_dist = normalised_df.groupby("language").agg(
        {"confidence": ["min", "max", "median", "skew", "mean"], "clean_message": "count"}
    )
    lang_dist = lang_dist.rename(columns={"clean_message": "number_of_items"})
    return lang_dist
