"""Creating artifacts for specific exports of the tagging data.

Include persisting csv files that can be used for manual
configuration and analysis.
"""
from typing import Optional

import pandas as pd
import tentaclio

from phoenix.common import artifacts
from phoenix.tag import constants, feature, object_filters


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
    ten_percent = min(constants.TO_LABEL_CSV_MAX, round(posts_to_scrape.shape[0] * 0.1))

    return posts_to_scrape[:ten_percent]


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


def features_for_labeling(
    ARTIFACTS_BASE_URL: str, all_features_df: pd.DataFrame, export_type: Optional[str]
) -> pd.DataFrame:
    """Export the features for labelling.

    Arguments:
        ARTIFACTS_BASE_URL: Folder to persist csv to. Any URL supported by tentaclio.
        all_features_df: see docs/schemas/features.md
        export_type: export filter see `phoenix/tag/object_filters.py::export`

    Returns:
        persisted features

    """
    if export_type:
        df = object_filters.export(all_features_df, export_type)
    else:
        export_type = "all"
        df = all_features_df

    df_to_label = feature.get_features_to_label(df)

    with tentaclio.open(ARTIFACTS_BASE_URL + f"{export_type}_features_to_label.csv", "w") as fb:
        df_to_label.to_csv(fb)

    return df_to_label
