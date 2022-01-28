"""Creating artifacts for specific exports of the tagging data.

Include persisting csv files that can be used for manual
configuration and analysis.
"""
from typing import Optional

import pandas as pd
import tentaclio

from phoenix.common import artifacts
from phoenix.tag import constants


def get_posts_to_scrape(posts_df: pd.DataFrame) -> pd.DataFrame:
    """Get posts to scrape.

    Get the top 10% of posts that will be manually scraped for comments.
    Order the posts by interactions.

    Arguments:
        posts_df: see docs/schemas/facebook_posts.md

    Returns:
        A subset of the columns needed for manual scraping.

    """
    posts_to_scrape = posts_df[
        [
            "phoenix_post_id",
            "account_name",
            "post_created",
            "text",
            "total_interactions",
            "post_url",
            "scrape_url",
        ]
    ]
    posts_to_scrape.sort_values(by="total_interactions", inplace=True, ascending=False)
    # Ten percent
    sample_len = min(constants.TO_LABEL_CSV_MAX, round(posts_to_scrape.shape[0] * 0.1))
    # If percent is smaller then the minimum
    if sample_len < constants.TO_LABEL_CSV_MIN:
        min_sample = min(constants.TO_LABEL_CSV_MIN, posts_to_scrape.shape[0])
        sample_len = min_sample

    return posts_to_scrape[:sample_len]


def persist_posts_to_scrape(
    posts_to_scrape: pd.DataFrame, tagging_url: str, dashboard_url: Optional[str] = None
):
    """Persist the posts to scrape."""
    artifacts.utils.create_folders_if_needed(tagging_url)
    with tentaclio.open(tagging_url, "w") as fb:
        posts_to_scrape.to_csv(fb)

    # The dashboard URL is optional
    if not dashboard_url:
        return

    artifacts.utils.create_folders_if_needed(tagging_url)
    with tentaclio.open(dashboard_url, "w") as fb:
        posts_to_scrape.to_csv(fb)


def get_all_features_for_export(features_df: pd.DataFrame) -> pd.DataFrame:
    """Normalise and transform the features dataframe so it can be persisted correctly."""
    features_df["object_id"] = features_df["object_id"].astype(str)
    features_df["features"] = features_df["features"].astype(str)
    return features_df[["object_id", "object_type", "features", "features_count"]]


def get_objects_for_export(objects_df: pd.DataFrame) -> pd.DataFrame:
    """Normalise and transform the objects dataframe so it can be persisted correctly."""
    objects_df["object_id"] = objects_df["object_id"].astype(str)
    objects_df["language_from_api"] = objects_df["language_from_api"].astype(str)
    objects_df["features"] = objects_df["features"].apply(lambda x: list(map(str, x)))
    return objects_df
