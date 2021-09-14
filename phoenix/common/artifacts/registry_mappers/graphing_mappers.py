"""Registry Graph mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


# graphing used to match the naming the phoenix repo
GRAPHING_PIPELINE_BASE = f"{shared_urls.TAGGING_PIPELINE_BASE}graphing/"

MAPPERS: MapperDict = {
    # Retweet
    "graphing_runs-retweet_pulled": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "retweet_pulled.parquet"
    ),
    "graphing_runs-retweet_input": partial(url_mapper, shared_urls.GROUP_BY_TWEETS),
}
