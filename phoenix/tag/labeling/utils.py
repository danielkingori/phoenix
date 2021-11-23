"""Utility functions for the labelling submodule."""


def get_account_object_type(object_type: str) -> str:
    """Get an account_object_type from the object_type."""
    object_type_mapping = {
        "facebook_posts": "facebook_pages",
        "tweets": "twitter_handles",
        "youtube_videos": "youtube_channels",
    }
    account_object_type = object_type_mapping.get(object_type, "")
    if not account_object_type:
        raise KeyError(f"No account object type found for {object_type}")

    return account_object_type
