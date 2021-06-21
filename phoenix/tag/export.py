"""Export functionallity."""
import pandas as pd
import tentaclio

from phoenix.tag import feature
from phoenix.tag.data_pull import constants


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


def get_items_for_export(items_df) -> pd.DataFrame:
    """Get the items for exporting."""
    items_df["object_id"] = items_df["object_id"].astype(str)
    items_df["language_from_api"] = items_df["language_from_api"].astype(str)
    return items_df


def filter_tag_data(df, export_type):
    """Filter tag data."""
    if "tweets" == export_type:
        return df[df["object_type"] == constants.OBJECT_TYPE_TWEET]

    if "facebook_posts" == export_type:
        return df[df["object_type"] == constants.OBJECT_TYPE_FACEBOOK_POST]

    if "key_facebook_posts" == export_type:
        return df[
            (df["has_key_feature"].isin([True]))
            & (df["object_type"] == constants.OBJECT_TYPE_FACEBOOK_POST)
        ]

    if "key_tweets" == export_type:
        key_tweets = df[
            (df["has_key_feature"].isin([True]))
            & (df["object_type"] == constants.OBJECT_TYPE_TWEET)
        ]
        return key_tweets[~key_tweets["is_retweet"]]

    if "key_objects" == export_type:
        return pd.concat(
            [filter_tag_data(df, "key_tweets"), filter_tag_data(df, "key_facebook_posts")]
        )

    raise ValueError(f"Export Type not supported: {export_type}")


def features_for_labeling(ARTIFACTS_BASE_URL, all_features_df, export_type):
    """Export the features for labeling."""
    if export_type:
        df = filter_tag_data(all_features_df, export_type)
    else:
        df = all_features_df

    df_to_label = feature.get_features_to_label(df)

    with tentaclio.open(ARTIFACTS_BASE_URL + f"{export_type}_features_to_label.csv", "w") as fb:
        df_to_label.to_csv(fb)
