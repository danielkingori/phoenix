"""Finalise run params for youtube comments."""
from typing import Optional, Union

from phoenix.common.run_params import general, utils
from phoenix.tag.finalise_run_params import dtypes


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
    """Create the Finalise run params for youtube_comments."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(general_run_params, object_type, year_filter, month_filter, final_url)

    return dtypes.FinaliseRunParams(
        general=general_run_params,
        urls=urls,
        include_objects_tensions=utils.normalise_bool(include_objects_tensions),
        include_sentiment=utils.normalise_bool(include_sentiment),
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
    object_type: str,
    year_filter: int,
    month_filter: int,
    final_url: Optional[str],
) -> dtypes.FinaliseRunParamsURLs:
    """Get FinaliseRunParamsURLs for youtube_comments."""
    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    if not final_url:
        final_url = art_url_reg.get_url("final-youtube_comments", url_config)
    return dtypes.FinaliseRunParamsURLs(
        config=url_config,
        input_dataset=art_url_reg.get_url("tagging_runs-youtube_comments_pulled", url_config),
        objects_tensions=art_url_reg.get_url("tagging_runs-objects_tensions", url_config),
        language_sentiment_objects=art_url_reg.get_url(
            "tagging_runs-language_sentiment_objects", url_config
        ),
        tagging_final=art_url_reg.get_url("tagging_runs-youtube_comments_final", url_config),
        final=final_url,
    )
