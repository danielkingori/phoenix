"""Init topic."""


def get_object_topics(topics_df, objects_df):
    """Get the objects topic dataframe.

    Parameters:
    topics_df:
        object_id: object_id
        object_type: object_type
        topic: string
        matched_features: List[string]
    objects_df:
        see docs/schemas/objects.md

    Returns:
        objects topics: see docs/schemas/objects.md#Object_topics
    """
    df = topics_df.groupby(["object_id", "object_type"])
    has_topics_df = (
        topics_df[topics_df["has_topic"].isin([True])]
        .groupby(["object_id", "object_type"])
        .first()
        .rename(columns={"has_topic": "has_topics"})
    )
    df = df.agg({"topic": list}).rename(columns={"topic": "topics"})
    df = df.join(has_topics_df["has_topics"]).fillna(False)
    objects_df = objects_df.set_index("object_id", drop=False)
    df = df.droplevel(["object_type"])
    df = objects_df.join(df)
    return df.reset_index(drop=True)
