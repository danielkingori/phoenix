"""Registry Final mappers."""
from functools import partial

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
        (FINAL_BASE + "facebook_posts/"),
    ),
    "final-facebook_posts_topics": partial(
        url_mapper,
        (FINAL_BASE + "facebook_posts_topics/"),
    ),
    "final-facebook_posts_classes": partial(
        url_mapper,
        (FINAL_BASE + "facebook_posts_classes/"),
    ),
    "final-tweets": partial(
        url_mapper,
        (FINAL_BASE + "tweets/"),
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
        (FINAL_BASE + "youtube_videos/youtube_videos_final.parquet"),
    ),
    "final-youtube_videos_topics": partial(
        url_mapper,
        (FINAL_BASE + "youtube_videos_topics/youtube_videos_topics_final.parquet"),
    ),
    "final-youtube_videos_classes": partial(
        url_mapper,
        (FINAL_BASE + "youtube_videos_classes/youtube_videos_classes_final.parquet"),
    ),
    "final-youtube_comments": partial(
        url_mapper,
        (FINAL_BASE + "youtube_comments/youtube_comments_final.parquet"),
    ),
    "final-youtube_comments_topics": partial(
        url_mapper,
        (FINAL_BASE + "youtube_comments_topics/youtube_comments_topics_final.parquet"),
    ),
    "final-youtube_comments_classes": partial(
        url_mapper,
        (FINAL_BASE + "youtube_comments_classes/youtube_comments_classes_final.parquet"),
    ),
    "final-accounts": partial(
        url_mapper,
        (FINAL_BASE + OBJECT_BASE + "_accounts/accounts_final.parquet"),
    ),
    "final-objects_accounts_classes": partial(
        url_mapper,
        (FINAL_BASE + OBJECT_BASE + "_objects_accounts_classes/accounts_classes_final.parquet"),
    ),
}
