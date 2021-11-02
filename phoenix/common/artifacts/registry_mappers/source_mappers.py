"""Source mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


MAPPERS: MapperDict = {
    "source-undp_events_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/undp_events-{RUN_DATETIME}.json"
    ),
    "source-acled_events_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/acled_events-{RUN_DATETIME}.json"
    ),
    # Facebook Posts
    "source-posts": partial(url_mapper, "source_runs/{RUN_DATE}/source-posts-{RUN_DATETIME}.json"),
    "source-fb_post_source_api_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/fb_post_source_api-{RUN_DATETIME}.ipynb"
    ),
    # Tweets
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
    "source-facebook_comments": partial(
        url_mapper, "source_runs/{RUN_DATE}/source-facebook_comments-{RUN_DATETIME}.json"
    ),
}
