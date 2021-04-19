"""Utils for artifacts module."""


def validate_artifact_url(
    suffix: str,
    artifacts_url: str,
) -> bool:
    """Validate an URL for Artifact.

    Raises:
        ValueError if invalid.
    """
    if not artifacts_url.endswith(suffix):
        url_msg = f"URL: {artifacts_url}"
        invalid_msg = f"is not valid must end with {suffix}"
        raise ValueError(f"{url_msg} {invalid_msg}")
    return True
