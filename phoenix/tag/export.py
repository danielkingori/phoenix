"""Export functionallity."""
import pandas as pd
import tentaclio

from phoenix.tag import feature, object_filters


def get_posts_to_scrape(posts_df) -> pd.DataFrame:
    """Get posts to scrape."""
    posts_to_scrape = posts_df[
        [
            "phoenix_post_id",
            "page_name",
            "post_created",
            "message",
            "total_interactions",
            "url",
            "scrape_url",
        ]
    ]
    posts_to_scrape.sort_values(by="total_interactions", inplace=True, ascending=False)
    ten_percent = round(posts_to_scrape.shape[0] * 0.1)

    return posts_to_scrape[:ten_percent]


def get_all_features_for_export(features_df) -> pd.DataFrame:
    """Get."""
    features_df["object_id"] = features_df["object_id"].astype(str)
    features_df["language_from_api"] = features_df["language_from_api"].astype(str)
    features_df["features"] = features_df["features"].astype(str)
    return features_df


def get_objects_for_export(objects_df) -> pd.DataFrame:
    """Get the objects for exporting."""
    objects_df["object_id"] = objects_df["object_id"].astype(str)
    objects_df["language_from_api"] = objects_df["language_from_api"].astype(str)
    objects_df["features"] = objects_df["features"].apply(lambda x: list(map(str, x)))
    return objects_df


def features_for_labeling(ARTIFACTS_BASE_URL, all_features_df, export_type):
    """Export the features for labeling."""
    if export_type:
        df = object_filters.export(all_features_df, export_type)
    else:
        df = all_features_df

    df_to_label = feature.get_features_to_label(df)

    with tentaclio.open(ARTIFACTS_BASE_URL + f"{export_type}_features_to_label.csv", "w") as fb:
        df_to_label.to_csv(fb)
