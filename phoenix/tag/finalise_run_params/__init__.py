"""Finalise Run Params."""
from typing import Optional, Union

from phoenix.tag.finalise_run_params import dtypes, youtube_videos


_registry_map = {"youtube_videos": youtube_videos.create}


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
    final_url: Optional[str],
    include_objects_tensions: Union[bool, str, None],
    include_sentiment: Union[bool, str, None],
) -> dtypes.FinaliseRunParams:
    """Create the finalisation run params for the object type."""
    if object_type not in _registry_map:
        raise RuntimeError(f"Object Type: {object_type} is not supported for finalisation.")
    create_fn = _registry_map[object_type]

    return create_fn(
        artifacts_environment_key=artifacts_environment_key,
        tenant_id=tenant_id,
        run_datetime_str=run_datetime_str,
        object_type=object_type,
        year_filter=year_filter,
        month_filter=month_filter,
        final_url=final_url,
        include_objects_tensions=include_objects_tensions,
        include_sentiment=include_sentiment,
    )
