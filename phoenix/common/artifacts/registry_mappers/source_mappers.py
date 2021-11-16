"""Source mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


SOURCE_BASE = "source_runs/{RUN_DATE}/"

MAPPERS: MapperDict = {
    "source-notebooks_base": partial(
        url_mapper, f"{SOURCE_BASE}" + "output_notebooks/{RUN_DATETIME}/"
    ),
    "source-undp_events_notebook": partial(
        url_mapper, f"{SOURCE_BASE}" + "undp_events-{RUN_DATETIME}.ipynb"
    ),
    "source-acled_events_notebook": partial(
        url_mapper, f"{SOURCE_BASE}" + "acled_events-{RUN_DATETIME}.ipynb"
    ),
    # Facebook Posts
    "source-posts": partial(url_mapper, "source_runs/{RUN_DATE}/source-posts-{RUN_DATETIME}.json"),
    "source-fb_post_source_api_notebook": partial(
        url_mapper, f"{SOURCE_BASE}" + "fb_post_source_api-{RUN_DATETIME}.ipynb"
    ),
    # Tweets
    "source-user_tweets": partial(
        url_mapper, f"{SOURCE_BASE}" + "source-user_tweets-{RUN_DATETIME}.json"
    ),
    "source-keyword_tweets": partial(
        url_mapper, f"{SOURCE_BASE}" + "source-keyword-{RUN_DATETIME}.json"
    ),
    "source-twitter_user_notebook": partial(
        url_mapper, f"{SOURCE_BASE}" + "twitter_user_timeline-{RUN_DATETIME}.ipynb"
    ),
    "source-twitter_keyword_notebook": partial(
        url_mapper, f"{SOURCE_BASE}" + "twitter_keyword_search-{RUN_DATETIME}.ipynb"
    ),
    "source-facebook_comments": partial(
        url_mapper, f"{SOURCE_BASE}" + "source-facebook_comments-{RUN_DATETIME}.json"
    ),
    "source-youtube_channels_from_channel_ids": partial(
        url_mapper,
        f"{SOURCE_BASE}" + "source-youtube_channels_from_channel_ids-{RUN_DATETIME}.json",
    ),
    "source-youtube_videos_from_channel_ids": partial(
        url_mapper,
        f"{SOURCE_BASE}" + "source-youtube_videos_from_channel_ids-{RUN_DATETIME}.json",
    ),
}
