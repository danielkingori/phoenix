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
