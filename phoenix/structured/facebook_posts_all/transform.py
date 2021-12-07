"""Facebook posts all transform."""
import json

import numpy as np
import pandas as pd
import prefect
import tentaclio

from phoenix.common import constants, pd_utils
from phoenix.structured.common import utils as structured_utils


@prefect.task
def transform_task(file_url) -> pd.DataFrame:
    """Transform task."""
    return execute(file_url)


def execute(file_url) -> pd.DataFrame:
    """From a file URL get the facebook_posts_all dataframe."""
    # Getting both the normalised and the read
    # This is because the read does a better job of typing
    # We are going to join them together in the normalisation
    source_file_name = structured_utils.get_source_file_name(file_url)
    with tentaclio.open(file_url) as file_io:
        js_obj = json.load(file_io)
        df_flattened = pd.json_normalize(js_obj)

    with tentaclio.open(file_url) as file_io:
        df_read = pd.read_json(file_io)

    df = normalise(df_read, df_flattened)
    df = structured_utils.add_file_cols(df, source_file_name)
    return df


def normalise(raw_df: pd.DataFrame, df_flattened: pd.DataFrame) -> pd.DataFrame:
    """Normalise the raw dataframe.

    The raw data is in JSON format and has a number of nested properties. As such
    we process two versions of raw data.

    1. raw_df: has the nested data, this uses `pd.read_json`
    2. df_flattened: this has all of the nested data flattened using `pd.json_normalize`

    The reason for this is ease of creating the final data structure:
    - `raw_df` (`pd.read_json`) has better dtypes for the non nested properties
    - `df_flattened` (`pd.json_normalize`) has the formatted columns for the nested data

    Args:
        raw_df: return of `pd.read_json` of the source file
        df_flattened: return of `pd.json_normalize` of the source file
    """
    df = raw_df.rename(pd_utils.camel_to_snake, axis="columns")
    df = df.rename(columns={"language_code": "language_from_api", "message": "text"})
    df_flattened = df_flattened.rename(pd_utils.camel_to_snake, axis="columns")
    df_flattened.columns = df_flattened.columns.str.replace(".", "_")
    df = merge_flattened(df, df_flattened)
    df = df[~df["text"].isna()]
    df = pd_utils.to_type("text", str, df)
    df = pd_utils.to_type("type", str, df)
    df = map_score(constants.FACEBOOK_POST_SORT_BY, df)
    df["created_at"] = df["date"].dt.tz_localize("UTC")
    df = structured_utils.add_filter_cols(df, df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated"]).dt.tz_localize("UTC")
    df["scrape_url"] = df["post_url"].str.replace(
        "https://www.facebook", "https://mbasic.facebook"
    )
    # URL post id
    df["url_post_id"] = df["post_url"].fillna("").str.extract(r"(\d+$)", flags=0, expand=False)
    return df.drop(
        columns=[
            "account",
            "date",
            "updated",
            "statistics",
            "media",
            "expanded_links",
            "branded_content_sponsor",
            "live_video_status",
            "legacyid",
        ],
        # Using ignore as missing data is not imporant
        errors="ignore",
    )


def map_score(sort_by_api: str, df: pd.DataFrame) -> pd.DataFrame:
    """Map score based on the sort by parameter of the request."""
    all_scores_mapping = {
        "total_interactions": "total_interactions",
        "overperforming": "overperforming_score",
        "interaction_rate": "interaction_rate",
        "underperforming": "underperforming_score",
    }
    for sort_by_match, col in all_scores_mapping.items():
        if sort_by_match == sort_by_api:
            df.rename(columns={"score": col}, inplace=True)
        else:
            df[col] = np.nan
        df[col] = df[col].astype(float)

    return df


def merge_flattened(df: pd.DataFrame, df_flattened: pd.DataFrame) -> pd.DataFrame:
    """Merged flattened dataframe with the non flattened.

    1. df: has the nested data, this uses `pd.read_json`
    2. df_flattened: this has all of the nested data flattened using `pd.json_normalize`

    The reason for this is ease of creating the final data structure:
    - `df` (`pd.read_json`) has better dtypes for the non nested properties
    - `df_flattened` (`pd.json_normalize`) has the formatted columns for the nested data

    Args:
        df: return of `pd.read_json` of the source file
        df_flattened: return of `pd.json_normalize` of the source file
    """
    to_add = [
        "platform_id",
        "account_name",
        "account_handle",
        "account_platform_id",
        "account_page_category",
        "account_page_admin_top_country",
        "account_page_description",
        "account_url",
        "account_page_created_date",
        "statistics_actual_like_count",
        "statistics_actual_comment_count",
        "statistics_actual_share_count",
        "statistics_actual_love_count",
        "statistics_actual_wow_count",
        "statistics_actual_haha_count",
        "statistics_actual_sad_count",
        "statistics_actual_angry_count",
        "statistics_actual_care_count",
    ]
    df[to_add] = df_flattened[to_add]
    # Some posts don't have an account this should be looked in to further
    # https://gitlab.com/howtobuildup/phoenix/-/issues/47
    df["account_platform_id"] = df["account_platform_id"].fillna(0).astype(int)
    df["account_page_created_date"] = pd.to_datetime(
        df["account_page_created_date"]
    ).dt.tz_localize("UTC")
    to_str = [
        "account_page_category",
        "account_page_admin_top_country",
        "account_page_description",
    ]

    for col in to_str:
        df[col] = df[col].astype(str)
    return df
