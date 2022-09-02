"""Data Pull run params for facebook feed."""
from phoenix.common.run_params import general
from phoenix.tag.data_pull.run_params import dtypes


def get_urls(
    general_run_params: general.GeneralRunParams,
    object_type: str,
    year_filter: int,
    month_filter: int,
) -> dtypes.DataPullRunParamsURLs:
    """Get DataPullRunParamsURLs for facebook feed."""
    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    return dtypes.DataPullRunParamsURLs(
        config=url_config,
        input_dataset=art_url_reg.get_url("tagging_runs-facebook_feed_input", url_config),
        pulled=art_url_reg.get_url("tagging_runs-facebook_feed_pulled", url_config),
        for_tagging=art_url_reg.get_url("tagging_runs-facebook_posts_for_tagging", url_config),
    )
