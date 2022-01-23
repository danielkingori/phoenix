"""Processing and config for facebook_posts_commentors graph."""

import pandas as pd

from phoenix.tag.graphing import processing_utilities


ARTIFACT_KEYS = [
    "final-facebook_comments_classes",
    "final-facebook_posts_classes",
    "final-accounts",
]


def process_post_nodes(final_facebook_posts_classes: pd.DataFrame) -> pd.DataFrame:
    """Process facebook posts to create set of nodes of type `post`."""
    cols_to_keep = [
        "object_id",
        "platform_id",
        "account_handle",
        "account_platform_id",
        "medium_type",
        "text",
        "class",
    ]
    cols_to_keep = cols_to_keep + [
        col for col in final_facebook_posts_classes.columns if "statistics" in col
    ]
    df = final_facebook_posts_classes[cols_to_keep]
    df = processing_utilities.reduce_concat_classes(df, ["object_id"], "class")
    return df
