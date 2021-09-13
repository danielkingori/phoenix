"""Registry Final mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


FINAL_BASE = "final/"

MAPPERS: MapperDict = {
    "final-facebook_posts": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_posts/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    )
}
