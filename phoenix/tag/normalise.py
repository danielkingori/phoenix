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


def join_fb_posts_tweets(posts_df, tweets_df) -> pd.DataFrame:
    """Join the facebook posts with the tweets."""
    tweets_formated = tweets_df[
        ["id_str", "clean_message", "full_text", "features", "features_count"]
    ]
    tweets_formated = tweets_formated.rename(columns={"id_str": "object_id", "full_text": "text"})
    tweets_formated["object_type"] = "tweets"
    posts_formated = posts_df[
        [
            "facebook_id",
            "clean_message",
            "message",
            "features",
            "features_count",
        ]
    ]
    posts_formated = posts_formated.rename(columns={"facebook_id": "object_id", "message": "text"})
    posts_formated["object_type"] = "facebook_posts"

    return pd.concat([tweets_formated, posts_formated], axis=0, ignore_index=True)
