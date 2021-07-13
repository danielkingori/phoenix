"""Init topic."""


def get_object_topics(topics_df):
    """Get the objects topic dataframe.

    Parameters:
    topics_df:
        object_id: object_id
        object_type: object_type
        topic: string
        matched_features: List[string]

    Returns:
    objects topics:
        object_id: object_id
        object_type: object_type
        topics: List[string]
    """
    df = topics_df.groupby(["object_id", "object_type"])
    return df.agg({"topic": list}).rename(columns={"topic": "topics"})
