"""Registry Tagging mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


TAGGING_FACEBOOK_POSTS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}facebook_posts/"
FOR_TAGGING_SUFFIX = "for_tagging/"
TAGGING_FACEBOOK_POSTS_FOR_TAGGING = TAGGING_FACEBOOK_POSTS + FOR_TAGGING_SUFFIX
TAGGING_PIPELINE_BASE = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}" + "{OBJECT_TYPE}/"
TAGGING_MANUALLY_USABLE_BASE = TAGGING_PIPELINE_BASE + "manually_usable/"
TAGGING_SENTIMENT_BASE = TAGGING_PIPELINE_BASE + "sentiment_analysis/"

MAPPERS: MapperDict = {
    # Facebook Posts
    "tagging_runs-facebook_posts_input": partial(url_mapper, shared_urls.GROUP_BY_FACEBOOK_POSTS),
    "tagging_runs-facebook_posts_pulled": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS + "facebook_posts_pulled.parquet"
    ),
    "tagging_runs-facebook_posts_for_tagging": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS_FOR_TAGGING + "facebook_posts_for_tagging.parquet"
    ),
    "tagging_runs-facebook_posts_final": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS + "facebook_posts_final.parquet"
    ),
    "tagging_runs-facebook_posts_to_scrape": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS + "posts_to_scrape.csv"
    ),
    # Utils
    "tagging_runs-pipeline_base": partial(url_mapper, TAGGING_PIPELINE_BASE),
    "tagging_runs-pipeline_manually_usable_base": partial(
        url_mapper, TAGGING_MANUALLY_USABLE_BASE
    ),
    # Features
    "tagging_runs-features_for_tagging": partial(
        url_mapper, TAGGING_PIPELINE_BASE + FOR_TAGGING_SUFFIX
    ),
    "tagging_runs-key_objects": partial(url_mapper, TAGGING_PIPELINE_BASE + "key_objects.parquet"),
    "tagging_runs-objects": partial(url_mapper, TAGGING_PIPELINE_BASE + "objects.parquet"),
    "tagging_runs-all_features": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "all_features.parquet"
    ),
    # Topics
    "tagging_runs-topics": partial(url_mapper, TAGGING_PIPELINE_BASE + "topics.parquet"),
    "tagging_runs-objects_topics_csv": partial(
        url_mapper, TAGGING_MANUALLY_USABLE_BASE + "objects_topics.csv"
    ),
    "tagging_runs-topics_csv": partial(url_mapper, TAGGING_MANUALLY_USABLE_BASE + "topics.csv"),
    # Tensions
    "tagging_runs-objects_tensions": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "objects_tensions.parquet"
    ),
    # Sentiment
    "tagging_runs-async_job_group": partial(
        url_mapper, TAGGING_SENTIMENT_BASE + "async_job_group.json"
    ),
    "tagging_runs-comprehend_base": partial(
        url_mapper, TAGGING_SENTIMENT_BASE + "comprehend_jobs/"
    ),
    "tagging_runs-language_sentiment_objects": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "language_sentiment_objects.parquet"
    ),
}
