"""Registry Tagging mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


# Base
TAGGING_PIPELINE_BASE = shared_urls.TAGGING_PIPELINE_BASE

MAPPERS: MapperDict = {
    "legacy-language_sentiment_objects": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "language_sentiment_objects.parquet"
    ),
}
