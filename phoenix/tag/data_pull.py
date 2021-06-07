"""Data pulling functionallity."""
import re

import pandas as pd
import tentaclio


def crowdtangle_csvs(url_to_folder: str) -> pd.DataFrame:
    """Get all the csvs and return a normalised facebook posts."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        with tentaclio.open(entry) as file_io:
            df = pd.read_csv(file_io, index_col=None, header=0)
            li.append(df)

    posts_df = pd.concat(li, axis=0, ignore_index=True)
    return normalise_fb_posts(posts_df)


def normalise_fb_posts(raw_df: pd.DataFrame):
    """normalise_fb_posts raw dataframe."""
    df = raw_df.rename(map_names, axis="columns")
    df = to_type("message", str, df)
    df = to_type("page_description", str, df)
    df["total_interactions"] = (
        df["total_interactions"].replace(to_replace=r",", value="", regex=True).astype(int)
    )
    return df


def to_type(column_name: str, astype, df: pd.DataFrame):
    """Name column string."""
    df[column_name] = df[column_name].astype(astype)
    return df


def map_names(name: str):
    """Map column names."""
    return re.sub(r"\W+", "_", name.lower())


def twitter_json(url_to_folder: str) -> pd.DataFrame:
    """Get all the csvs and return a normalised facebook posts."""
    li = []
    for entry in tentaclio.listdir(url_to_folder):
        with tentaclio.open(entry) as file_io:
            df = pd.read_json(file_io)
            li.append(df)

    posts_df = pd.concat(li, axis=0, ignore_index=True)
    return normalise_tweets(posts_df)


def normalise_tweets(raw_df: pd.DataFrame):
    """normalise_tweets raw dataframe."""
    df = to_type("full_text", str, raw_df)
    return df
