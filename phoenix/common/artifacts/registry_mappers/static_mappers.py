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
    "static-g_sheet_folder_ids": partial(url_mapper, CONFIG_BASE + "google_sheet_folder_ids.csv"),
}
