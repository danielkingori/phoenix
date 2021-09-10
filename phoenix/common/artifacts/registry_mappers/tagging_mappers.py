"""Registry Tagging mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


TAGGING_FACEBOOK_POSTS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}facebook_posts/"
TAGGING_FACEBOOK_POSTS_FOR_TAGGING = TAGGING_FACEBOOK_POSTS + "for_tagging/"

MAPPERS: MapperDict = {
    # Facebook Posts
    "tagging_runs-facebook_posts_input": partial(url_mapper, shared_urls.GROUP_BY_FACEBOOK_POSTS),
    "tagging_runs-facebook_posts_pulled": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS + "facebook_posts_pulled.parquet"
    ),
    "tagging_runs-facebook_posts_for_tagging": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS_FOR_TAGGING + "facebook_posts_for_tagging.parquet"
    ),
}
