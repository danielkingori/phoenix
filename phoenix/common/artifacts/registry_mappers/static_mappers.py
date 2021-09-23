"""Registry Static mappers."""
from typing import Any, Dict

from functools import partial

from phoenix.common import artifacts
from phoenix.common.artifacts import registry_environment as reg_env
from phoenix.common.artifacts.registry_mappers import artifact_keys
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


def static_url_mapper(
    format_str: str,
    artifact_key: artifact_keys.ArtifactKey,
    url_config: Dict[str, Any],
    environment_key: reg_env.Environments = reg_env.DEFAULT_ENVIRONMENT_KEY,
):
    """Static URL Mapper.

    Currently the static mapping is from files in git control.
    """
    prefix = f"{artifacts.urls.get_static_config()}"
    url_str_formated = format_str.format(**url_config)
    return f"{prefix}{url_str_formated}"


CONFIG_BASE = "config/"


MAPPERS: MapperDict = {
    # Retweet
    "static-twitter_users": partial(url_mapper, CONFIG_BASE + "twitter_query_users.csv"),
    # Custom Models
    "static-custom_models_tension_classifier_base": partial(
        url_mapper, "custom_models/tension_classifier/"
    ),
}
