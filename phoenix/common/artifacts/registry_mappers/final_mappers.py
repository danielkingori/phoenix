"""Registry Final mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


FINAL_BASE = "final/"
OBJECT_BASE = "{OBJECT_TYPE}"

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
        (FINAL_BASE + "facebook_posts/facebook_posts_final.parquet"),
    ),
    "final-facebook_posts_topics": partial(
        url_mapper,
        (FINAL_BASE + "facebook_posts_topics/facebook_posts_topics_final.parquet"),
    ),
    "final-facebook_posts_classes": partial(
        url_mapper,
        (FINAL_BASE + "facebook_posts_classes/facebook_posts_classes_final.parquet"),
    ),
    "final-tweets": partial(
        url_mapper,
        (FINAL_BASE + "tweets/tweets_final.parquet"),
    ),
    "final-tweets_topics": partial(
        url_mapper,
        (FINAL_BASE + "tweets_topics/tweets_topics_final.parquet"),
    ),
    "final-tweets_classes": partial(
        url_mapper,
        (FINAL_BASE + "tweets_classes/tweets_classes_final.parquet"),
    ),
    "final-facebook_comments": partial(
        url_mapper,
        (FINAL_BASE + "facebook_comments/facebook_comments_final.parquet"),
    ),
    "final-facebook_comments_topics": partial(
        url_mapper,
        (FINAL_BASE + "facebook_comments_topics/facebook_comments_topics_final.parquet"),
    ),
    "final-facebook_comments_classes": partial(
        url_mapper,
        (FINAL_BASE + "facebook_comments_classes/facebook_comments_classes_final.parquet"),
    ),
    "final-youtube_videos": partial(
        url_mapper,
        (
            FINAL_BASE + "youtube_videos/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-youtube_videos_topics": partial(
        url_mapper,
        (
            FINAL_BASE + "youtube_videos_topics/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-youtube_videos_classes": partial(
        url_mapper,
        (
            FINAL_BASE + "youtube_videos_classes/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-accounts": partial(
        url_mapper,
        (
            FINAL_BASE + OBJECT_BASE + "_accounts/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
    "final-objects_accounts_classes": partial(
        url_mapper,
        (
            FINAL_BASE + OBJECT_BASE + "_objects_accounts_classes/"
            f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
            "{YEAR_FILTER}-{MONTH_FILTER}.parquet"
        ),
    ),
}
