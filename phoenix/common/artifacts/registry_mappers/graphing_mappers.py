"""Registry Graph mappers."""
from typing import Any, Dict

from functools import partial

from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts.registry_mappers import artifact_keys, shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


def dashboard_url_mapper(
    format_str: str,
    artifact_key: artifact_keys.ArtifactKey,
    url_config: Dict[str, Any],
    environment_key: reg_env.Environments = reg_env.DEFAULT_ENVIRONMENT_KEY,
):
    """Dashboard url mapper.

    The dashboard artifacts are saved into a separate bucket with different permissions.
    As such they return None unless the env variable is set and the environment_key is production.
    See phoenix/common/artifacts/registry_environment.py::dashboard_url_prefix
    for more information.
    """
    prefix = reg_env.dashboard_url_prefix(artifact_key, url_config, environment_key)
    if not prefix:
        return None
    url_str_formated = format_str.format(**url_config)
    return f"{prefix}{url_str_formated}"


# graphing used to match the naming the phoenix repo
GRAPHING_PIPELINE_BASE = f"{shared_urls.TAGGING_PIPELINE_BASE}graphing/"

MAPPERS: MapperDict = {
    # Retweet
    "graphing_runs-retweet_pulled": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "retweet_pulled.parquet"
    ),
    "graphing_runs-retweet_input": partial(url_mapper, shared_urls.GROUP_BY_TWEETS),
    "graphing_runs-retweet_output_graph": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "retweet_graph.html"
    ),
    "graphing_runs-retweet_dashboard_graph": partial(
        dashboard_url_mapper, GRAPHING_PIPELINE_BASE + "retweet_graph.html"
    ),
    # Facebook topics
    "graphing_runs-facebook_topics_graph_pulled": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "topics_graph_pulled.parquet"
    ),
    "graphing_runs-facebook_topics_output_graph": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "topics_graph.html"
    ),
    "graphing_runs-facebook_topics_dashboard_graph": partial(
        dashboard_url_mapper, GRAPHING_PIPELINE_BASE + "topics_graph.html"
    ),
}
