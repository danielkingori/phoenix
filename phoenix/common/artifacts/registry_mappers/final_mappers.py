"""Registry Final mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


FINAL_BASE = "final/"

MAPPERS: MapperDict = {
    "final-acled_events": partial(
        url_mapper,
        (FINAL_BASE + "acled_events/persisted.parquet"),
    ),
    "final-undp_events": partial(
        url_mapper,
        (FINAL_BASE + "undp_events/persisted.parquet"),
    ),
    "final-facebook_posts": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_posts/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-facebook_posts_topics": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_posts_topics/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-facebook_posts_classes": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_posts_classes/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-tweets": partial(
        url_mapper,
        (
            FINAL_BASE + "tweets/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-tweets_topics": partial(
        url_mapper,
        (
            FINAL_BASE + "tweets_topics/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-tweets_classes": partial(
        url_mapper,
        (
            FINAL_BASE + "tweets_classes/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-facebook_comments": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_comments/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-facebook_comments_topics": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_comments_topics/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-facebook_comments_classes": partial(
        url_mapper,
        (
            FINAL_BASE + "facebook_comments_classes/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-youtube_videos": partial(
        url_mapper,
        (
            FINAL_BASE + "youtube_videos/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
}
