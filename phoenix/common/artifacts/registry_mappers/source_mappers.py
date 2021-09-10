"""Source mappers."""
from typing import Dict

from functools import partial

from phoenix.common.artifacts.registry_mappers.artifact_keys import ArtifactKey
from phoenix.common.artifacts.registry_mappers.default_url_mapper import (
    ArtifactURLMapper,
    url_mapper,
)


MAPPERS: Dict[ArtifactKey, ArtifactURLMapper] = {
    # Facebook Posts
    "source-posts": partial(url_mapper, "source_runs/{RUN_DATE}/source-posts-{RUN_DATETIME}.json"),
    "source-fb_post_source_api_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/fb_post_source_api-{RUN_DATETIME}.ipynb"
    ),
    "source-user_tweets": partial(
        url_mapper, "source_runs/{RUN_DATE}/source-user_tweets-{RUN_DATETIME}.json"
    ),
    "source-keyword_tweets": partial(
        url_mapper, "source_runs/{RUN_DATE}/source-keyword-{RUN_DATETIME}.json"
    ),
    "source-twitter_user_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/twitter_user_timeline-{RUN_DATETIME}.ipynb"
    ),
    "source-twitter_keyword_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/twitter_keyword_search-{RUN_DATETIME}.ipynb"
    ),
}
