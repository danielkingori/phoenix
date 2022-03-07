"""Finalise run params for youtube comments topics."""
from typing import Optional, Union

from phoenix.common.run_params import general, utils
from phoenix.tag.finalise_run_params import topics_dtypes


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
    final_url: Optional[str],
    rename_topic_to_class: Union[bool, str, None],
) -> topics_dtypes.TopicsFinaliseRunParams:
    """Create the TopicsFinaliseRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(
        general_run_params,
        object_type,
        year_filter,
        month_filter,
        final_url,
        rename_topic_to_class,
    )

    return topics_dtypes.TopicsFinaliseRunParams(
        general=general_run_params,
        urls=urls,
        rename_topic_to_class=utils.normalise_bool(rename_topic_to_class),
    )


def _get_urls(
    general_run_params: general.GeneralRunParams,
    object_type: str,
    year_filter: int,
    month_filter: int,
    final_url: Optional[str],
    rename_topic_to_class: Union[bool, str, None],
) -> topics_dtypes.TopicsFinaliseRunParamsURLs:
    """Get TopicsFinaliseRunParamsURLs."""
    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    if not final_url and not rename_topic_to_class:
        final_url = art_url_reg.get_url("final-youtube_comments_topics", url_config)
    if not final_url and rename_topic_to_class:
        final_url = art_url_reg.get_url("final-youtube_comments_classes", url_config)

    if rename_topic_to_class:
        tagging_final = art_url_reg.get_url(
            "tagging_runs-youtube_comments_classes_final", url_config
        )
    else:
        tagging_final = art_url_reg.get_url(
            "tagging_runs-youtube_comments_topics_final", url_config
        )

    if not final_url:
        raise RuntimeError("Final URL is not set.")
    return topics_dtypes.TopicsFinaliseRunParamsURLs(
        config=url_config,
        input_dataset=art_url_reg.get_url("tagging_runs-youtube_comments_final", url_config),
        topics=art_url_reg.get_url("tagging_runs-topics", url_config),
        tagging_final=tagging_final,
        final=final_url,
    )
