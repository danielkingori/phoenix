"""Process for features to objects."""
import pandas as pd


def get_feature_objects(exploded_features_df) -> pd.DataFrame:
    """Get the objects with key features."""
    g = exploded_features_df.groupby(level=0)
    objects_features = g.agg({"features": list, "features_count": list})
    return objects_features


def join_all_objects(all_objects, objects_features):
    """Join and finalise object with features."""
    objects = all_objects.copy()
    objects = objects.set_index("object_id", drop=False)
    objects = objects.join(objects_features[["features", "features_count"]])
    objects = objects.reset_index(drop=True)
    return objects


def finalise(all_objects, exploded_features_df) -> pd.DataFrame:
    """Get the final features and objects dataframes."""
    df = exploded_features_df.copy()
    objects_features = get_feature_objects(df)
    objects = join_all_objects(all_objects, objects_features)
    return objects
