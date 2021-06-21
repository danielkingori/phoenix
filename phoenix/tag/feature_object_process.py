"""Process for features to objects."""
from typing import Tuple

import pandas as pd

from phoenix.tag import feature, object_filters


def get_key_objects(exploded_features_is_key_df, features_key: str = "features") -> pd.DataFrame:
    """Get the objects with key features."""
    key_objects = group_key_objects(exploded_features_is_key_df, features_key)
    key_objects = object_filters.get_all_key_objects(key_objects)
    key_objects["is_key_object"] = True
    return key_objects


def group_key_objects(exploded_features_is_key_df, features_key: str = "features") -> pd.DataFrame:
    """Group the key objects."""
    df = exploded_features_is_key_df.copy()
    return (
        df[df["is_key_feature"].isin([True])]
        .groupby(level=0)
        .first()
        .drop(columns=["features", "features_count"])
        .rename(columns={"is_key_feature": "has_key_feature"})
    )


def get_feature_objects(exploded_features_df, features_key: str = "features") -> pd.DataFrame:
    """Get the objects with key features."""
    g = exploded_features_df.groupby(level=0)
    objects_features = g.agg({"features": list, "features_count": list})
    return objects_features


def features_with_is_key_feature(exploded_features_df, key_objects):
    """Return exploded_features_df with is_key_feature."""
    return exploded_features_df.join(key_objects[["is_key_object"]]).fillna(False)


def join_all_objects(all_objects, key_objects, objects_features):
    """Join and finalise object with key objects."""
    objects = all_objects.copy()
    objects = objects.set_index("object_id", drop=False)
    objects = objects.join(key_objects[["is_key_object"]]).fillna(False)
    objects = objects.join(objects_features[["features", "features_count"]])
    objects = objects.reset_index(drop=True)
    return objects


def finalise(
    all_objects, exploded_features_df, features_key: str = "features"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Get the final features and objects dataframes."""
    df = exploded_features_df.copy()
    df["is_key_feature"] = feature.key_features(df[[features_key]])
    key_objects = get_key_objects(df)
    df = features_with_is_key_feature(df, key_objects)
    objects_features = get_feature_objects(df)
    objects = join_all_objects(all_objects, key_objects, objects_features)
    key_objects_result = object_filters.get_key_objects(objects)
    return objects, key_objects_result, df
