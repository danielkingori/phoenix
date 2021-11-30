"""Object Classes functionality."""


def join(classes_df, objects_df):
    """Join classes df to the objects df.

    Parameters:
    classes_df:
        object_id: object_id
        object_type: object_type
        class: string
        matched_features: List[string]
    objects_df:
        see docs/schemas/objects.md

    Returns:
    objects classes:
        columns in objects ....
        classes: List[string]
        has_classes: boolean
    """
    df = classes_df.groupby(["object_id", "object_type"])
    has_classes_df = (
        classes_df[classes_df["has_class"]]
        .groupby(["object_id", "object_type"])
        .first()
        .rename(columns={"has_class": "has_classes"})
    )
    df = df.agg({"class": list}).rename(columns={"class": "classes"})
    df = df.join(has_classes_df["has_classes"]).fillna(False)
    objects_df = objects_df.set_index("object_id", drop=False)
    df = df.droplevel(["object_type"])
    df = objects_df.join(df)
    return df.reset_index(drop=True)
