"""Registry Static mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


CONFIG_BASE = "config/"


MAPPERS: MapperDict = {
    # Retweet
    "static-twitter_users": partial(url_mapper, CONFIG_BASE + "twitter_query_users.csv"),
    "static-twitter_keywords": partial(url_mapper, CONFIG_BASE + "twitter_query_keywords.csv"),
    # Custom Models
    "static-custom_models_tension_classifier_base": partial(
        url_mapper, "custom_models/tension_classifier/"
    ),
    "static-custom_models_tension_classifier_data": partial(
        url_mapper, CONFIG_BASE + "tension_classifier_data/"
    ),
    # Backport neccesity for sfm config, delete me BEFORE 2022-01-15
    "static-legacy-sfm-config": partial(
        url_mapper, CONFIG_BASE + "single_feature_match_topic_config.csv"
    ),
    "static-youtube_channels": partial(url_mapper, CONFIG_BASE + "youtube_channels.csv"),
}
