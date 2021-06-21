"""Tag module."""
import pandas as pd
import tentaclio

from phoenix.common import artifacts
from phoenix.tag import language


def merge(for_tagging_folder):
    """Merge tagging data."""
    li = []
    for entry in tentaclio.listdir(for_tagging_folder):
        df = artifacts.dataframes.get(entry).dataframe
        li.append(df)
    df = pd.concat(li, axis=0, ignore_index=True)
    df["object_id"] = df["object_id"].astype(str)
    return df


def execute(given_df: pd.DataFrame, text_key: str = "text") -> pd.DataFrame:
    """Tag Data."""
    df = given_df.copy()
    df["clean_text"] = clean_text(df[text_key])
    df[["language", "confidence"]] = language.execute(df["clean_text"])
    return df


def clean_text(text_ser) -> pd.Series:
    """Clean text."""
    return text_ser.replace(to_replace=r"https?:\/\/.*[\r\n]*", value="", regex=True)


def language_distribution(normalised_df) -> pd.DataFrame:
    """Get a distribution of languages."""
    lang_dist = normalised_df.groupby("language").agg(
        {"confidence": ["min", "max", "median", "skew", "mean"], "clean_text": "count"}
    )
    lang_dist = lang_dist.rename(columns={"clean_text": "number_of_items"})
    return lang_dist


def join_fb_posts_tweets(posts_df, tweets_df) -> pd.DataFrame:
    """Join the facebook posts with the tweets."""
    tweets_formated = tweets_df[
        ["id_str", "clean_text", "full_text", "features", "features_count", "has_key_feature"]
    ]
    tweets_formated = tweets_formated.rename(columns={"id_str": "object_id", "full_text": "text"})
    tweets_formated["object_type"] = "tweets"
    posts_formated = posts_df[
        [
            "facebook_id",
            "clean_text",
            "text",
            "features",
            "features_count",
            "has_key_feature",
        ]
    ]
    posts_formated = posts_formated.rename(columns={"facebook_id": "object_id", "text": "text"})
    posts_formated["object_type"] = "facebook_posts"


def is_unofficial_retweet(text_ser: pd.Series) -> pd.Series:
    """Detect if the text is an unofficial retweet.

    According to the twitter docs this is
    is when someone starts the message with "RT".
    https://help.twitter.com/en/using-twitter/retweet-faqs
    """
    retweet_re = r"(^RT)"
    return text_ser.str.match(retweet_re)
