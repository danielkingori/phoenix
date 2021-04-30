"""Tag module."""
import pandas as pd

from phoenix.tag import language, text_features_analyser


PROCESSOR_NAME = "tag"


def tag_dataframe(given_df: pd.DataFrame, message_key: str = "message") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    # TODO: general clean message: `cleanMessage`
    df[["language", "confidence"]] = language.execute(df[message_key])
    # TODO: arabicMessage (translation): `arabicMessage`
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[[message_key, "language"]], message_key)
    # TODO: sentiment: `sentiment`
    # TODO: topics: `topics`
    # TODO: has key feature: `hasKeyFeature`
    return df
