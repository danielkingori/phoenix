"""Base mappers."""
from typing import Dict

from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.artifact_keys import ArtifactKey
from phoenix.common.artifacts.registry_mappers.default_url_mapper import (
    ArtifactURLMapper,
    url_mapper,
)


MAPPERS: Dict[ArtifactKey, ArtifactURLMapper] = {
    # Facebook Posts
    "base-grouped_by_posts": partial(
        url_mapper, shared_urls.GROUP_BY_FACEBOOK_POSTS + "posts-{RUN_DATETIME}.json"
    ),
    # Twitter Tweets
    "base-grouped_by_user_tweets": partial(
        url_mapper, shared_urls.GROUP_BY_TWEETS + "user_tweets-{RUN_DATETIME}.json"
    ),
    "base-grouped_by_keyword_tweets": partial(
        url_mapper, shared_urls.GROUP_BY_TWEETS + "keyword_tweets-{RUN_DATETIME}.json"
    ),
}
