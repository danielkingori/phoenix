"""Tag module."""
import pandas as pd

from phoenix.tag import language, text_features_analyser


PROCESSOR_NAME = "tag"


def tag_dataframe(given_df: pd.DataFrame, message_key: str = "message") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    df["clean_message"] = clean_message(df[message_key])
    df[["language", "confidence"]] = language.execute(df["clean_message"])
    # TODO: arabicMessage (translation): `arabicMessage`
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[["clean_message", "language"]], "clean_message")
    # TODO: sentiment: `sentiment`
    # TODO: topics: `topics`
    # TODO: has key feature: `hasKeyFeature`
    return df


def tag_features(given_df: pd.DataFrame, message_key: str = "message") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    df["clean_message"] = clean_message(df[message_key])
    df[["language", "confidence"]] = language.execute(df["clean_message"])
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[["clean_message", "language"]], "clean_message")
    return df


def clean_message(message_ser) -> pd.Series:
    """Clean message."""
    return message_ser.replace(to_replace=r"https?:\/\/.*[\r\n]*", value="", regex=True)


def explode_features(given_df: pd.DataFrame):
    """Explode dataframe by the features_index."""
    df = given_df.copy()
    df["features_count"] = text_features_analyser.ngram_count(df[["features"]])
    df["features_1_count"] = df["features_count"]
    df["features_index"] = text_features_analyser.features_index(
        df[["features_count", "features_1_count"]]
    )
    ex_df = df.explode("features_index")
    ex_df["features"] = ex_df["features_index"].str[1]
    ex_df["features_count"] = ex_df["features_index"].str[2].fillna(0).astype(int)
    ex_df["index"] = ex_df.index
    return ex_df.drop(
        [
            "features_index",
        ],
        axis=1,
    )
