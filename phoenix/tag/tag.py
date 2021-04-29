"""Tag module."""
import pandas as pd

from phoenix.tag import language


PROCESSOR_NAME = "tag"


def tag_dataframe(given_df: pd.DataFrame, message_key: str = "message") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    df[["language", "confidence"]] = language.execute(df[message_key])
    return df
