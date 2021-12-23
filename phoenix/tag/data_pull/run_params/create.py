"""Create run params for data pull."""
from typing import Callable, Dict, Optional, Union

from phoenix.common.run_params import general, utils
from phoenix.tag.data_pull.run_params import dtypes, facebook_posts


_urls_registry_map: Dict[str, Callable] = {"facebook_posts": facebook_posts.get_urls}


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: Union[str, int, None],
    month_filter: Union[str, int, None],
    include_all_data_for_month: Union[bool, str, None],
) -> dtypes.DataPullRunParams:
    """Create the data_pull run params for the object type."""
    if object_type not in _urls_registry_map:
        raise RuntimeError(f"Object Type: {object_type} is not supported for data pull.")
    urls_create_fn = _urls_registry_map[object_type]

    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = urls_create_fn(general_run_params, object_type, year_filter, month_filter)
    # Allow for all the data in the month to be processed rather then filtering out data
    # that has been pulled
    applied_year_filter: Union[int, None] = utils.normalise_int(year_filter)
    applied_month_filter: Union[int, None] = utils.normalise_int(month_filter)
    include_all_data_for_month = utils.normalise_bool(include_all_data_for_month)
    if include_all_data_for_month:
        applied_year_filter = None
        applied_month_filter = None

    return dtypes.DataPullRunParams(
        general=general_run_params,
        urls=urls,
        year_filter=applied_year_filter,
        month_filter=applied_month_filter,
        include_all_data_for_month=include_all_data_for_month,
    )
