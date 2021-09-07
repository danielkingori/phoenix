"""Artifact Registry Mappers."""
from typing import Any, Dict
from typing_extensions import Literal, Protocol

from functools import partial

from phoenix.common.artifacts import registry_environment as reg_env


ArtifactKey = Literal[
    # Facebook posts
    "source-posts",
    "source-fb_post_source_api_notebook",
    "base-grouped_by_posts",
    # Tweets
    "source-user_tweets",
    "source-keyword_tweets",
    "source-twitter_user_notebook",
    "source-twitter_keyword_notebook",
    "base-grouped_by_user_tweets",
    "base-grouped_by_keyword_tweets",
]


class ArtifactURLMapper(Protocol):
    """Protocol for the artifactURLMapper."""

    def __call__(
        self,
        artifact_key: ArtifactKey,
        url_config: Dict[str, Any],
        environment_key: reg_env.Environments = reg_env.DEFAULT_ENVIRONMENT_KEY,
    ) -> str:
        """Protocol for the artifactURLMapper."""
        ...


def url_mapper(
    format_str: str,
    artifact_key: ArtifactKey,
    url_config: Dict[str, Any],
    environment_key: reg_env.Environments = reg_env.DEFAULT_ENVIRONMENT_KEY,
):
    """Generalised url mapper."""
    prefix = reg_env.default_url_prefix(artifact_key, url_config, environment_key)
    url_str_formated = format_str.format(**url_config)
    return f"{prefix}{url_str_formated}"


YEAR_MONTH_FILTER_DIRS = "year_filter={YEAR_FILTER}/month_filter={MONTH_FILTER}/"
DEFAULT_MAPPERS: Dict[ArtifactKey, ArtifactURLMapper] = {
    # Facebook Posts
    "source-posts": partial(url_mapper, "source_runs/{RUN_DATE}/source-posts-{RUN_DATETIME}.json"),
    "source-fb_post_source_api_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/fb_post_source_api-{RUN_DATETIME}.ipynb"
    ),
    "base-grouped_by_posts": partial(
        url_mapper,
        (
            "base/grouped_by_year_month/facebook_posts/"
            f"{YEAR_MONTH_FILTER_DIRS}"
            "posts-{RUN_DATETIME}.json"
        ),
    ),
    # Twitter Tweets
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
    "base-grouped_by_user_tweets": partial(
        url_mapper,
        (
            "base/grouped_by_year_month/tweets/"
            f"{YEAR_MONTH_FILTER_DIRS}"
            "user_tweets-{RUN_DATETIME}.json"
        ),
    ),
    "base-grouped_by_keyword_tweets": partial(
        url_mapper,
        (
            "base/grouped_by_year_month/tweets/"
            f"{YEAR_MONTH_FILTER_DIRS}"
            "keyword_tweets-{RUN_DATETIME}.json"
        ),
    ),
}
