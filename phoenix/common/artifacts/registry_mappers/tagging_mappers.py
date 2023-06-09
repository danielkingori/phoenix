"""Registry Tagging mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_url_mapper, shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


# Base
TAGGING_PIPELINE_BASE = shared_urls.TAGGING_PIPELINE_BASE
TAGGING_FOR_ANALYST_BASE = TAGGING_PIPELINE_BASE + "for_analyst/"
TAGGING_SENTIMENT_BASE = TAGGING_PIPELINE_BASE + "sentiment_analysis/"
FOR_TAGGING_SUFFIX = "for_tagging/"

# Facebook
TAGGING_FACEBOOK_POSTS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}facebook_posts/"
TAGGING_FACEBOOK_POSTS_FOR_TAGGING = TAGGING_FACEBOOK_POSTS + FOR_TAGGING_SUFFIX

# Tweets
TAGGING_TWEETS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}tweets/"
TAGGING_TWEETS_FOR_TAGGING = TAGGING_TWEETS + FOR_TAGGING_SUFFIX

# Facebook Comments
TAGGING_FACEBOOK_COMMENTS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}facebook_comments/"
TAGGING_FACEBOOK_COMMENTS_FOR_TAGGING = TAGGING_FACEBOOK_COMMENTS + FOR_TAGGING_SUFFIX

# Facebook
TAGGING_YOUTUBE_VIDEOS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}youtube_videos/"
TAGGING_YOUTUBE_VIDEOS_FOR_TAGGING = TAGGING_YOUTUBE_VIDEOS + FOR_TAGGING_SUFFIX
TAGGING_YOUTUBE_COMMENTS = f"tagging_runs/{shared_urls.YEAR_MONTH_FILTER_DIRS}youtube_comments/"
TAGGING_YOUTUBE_COMMENTS_FOR_TAGGING = TAGGING_YOUTUBE_COMMENTS + FOR_TAGGING_SUFFIX


MAPPERS: MapperDict = {
    # Notebooks
    "tagging_runs-output_notebook_base": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "output_notebooks/{RUN_DATETIME}/"
    ),
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
    "tagging_runs-facebook_posts_to_scrape_dashboard": partial(
        shared_url_mapper.dashboard_url_mapper, TAGGING_FACEBOOK_POSTS + "posts_to_scrape.csv"
    ),
    "tagging_runs-facebook_posts_topics_final": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS + "facebook_posts_topics_final.parquet"
    ),
    "tagging_runs-facebook_posts_classes_final": partial(
        url_mapper, TAGGING_FACEBOOK_POSTS + "facebook_posts_classes_final.parquet"
    ),
    "tagging_runs-custom_facebook_posts_to_scrape": partial(
        url_mapper,
        TAGGING_FACEBOOK_POSTS
        + "export_manual_scraping/{CUSTOM_PREFIX}facebook_posts_to_scrape.csv",
    ),
    # Tweets
    "tagging_runs-tweets_input": partial(url_mapper, shared_urls.GROUP_BY_TWEETS),
    "tagging_runs-tweets_pulled": partial(url_mapper, TAGGING_TWEETS + "tweets_pulled.parquet"),
    "tagging_runs-tweets_for_tagging": partial(
        url_mapper, TAGGING_TWEETS_FOR_TAGGING + "tweets_for_tagging.parquet"
    ),
    "tagging_runs-tweets_final": partial(url_mapper, TAGGING_TWEETS + "tweets_final.parquet"),
    "tagging_runs-tweets_topics_final": partial(
        url_mapper, TAGGING_TWEETS + "tweets_topics_final.parquet"
    ),
    "tagging_runs-tweets_classes_final": partial(
        url_mapper, TAGGING_TWEETS + "tweets_classes_final.parquet"
    ),
    # Facebook Comments
    "tagging_runs-facebook_comments_input": partial(
        url_mapper, shared_urls.GROUP_BY_FACEBOOK_COMMENTS
    ),
    "tagging_runs-facebook_comments_pulled": partial(
        url_mapper, TAGGING_FACEBOOK_COMMENTS + "facebook_comments_pulled.parquet"
    ),
    "tagging_runs-facebook_comments_for_tagging": partial(
        url_mapper, TAGGING_FACEBOOK_COMMENTS_FOR_TAGGING + "facebook_comments_for_tagging.parquet"
    ),
    "tagging_runs-facebook_comments_final": partial(
        url_mapper, TAGGING_FACEBOOK_COMMENTS + "facebook_comments_final.parquet"
    ),
    "tagging_runs-facebook_comments_topics_final": partial(
        url_mapper, TAGGING_FACEBOOK_COMMENTS + "facebook_comments_topics_final.parquet"
    ),
    "tagging_runs-facebook_comments_classes_final": partial(
        url_mapper, TAGGING_FACEBOOK_COMMENTS + "facebook_comments_classes_final.parquet"
    ),
    # Youtube
    "tagging_runs-youtube_videos_input": partial(
        url_mapper, shared_urls.GROUP_BY_YOUTUBE_SEARCH_VIDEOS
    ),
    "tagging_runs-youtube_videos_pulled": partial(
        url_mapper, TAGGING_YOUTUBE_VIDEOS + "youtube_videos_pulled.parquet"
    ),
    "tagging_runs-youtube_videos_for_tagging": partial(
        url_mapper, TAGGING_YOUTUBE_VIDEOS_FOR_TAGGING + "youtube_videos_for_tagging.parquet"
    ),
    "tagging_runs-youtube_videos_final": partial(
        url_mapper, TAGGING_YOUTUBE_VIDEOS + "youtube_videos_final.parquet"
    ),
    "tagging_runs-youtube_videos_topics_final": partial(
        url_mapper, TAGGING_YOUTUBE_VIDEOS + "youtube_videos_topics_final.parquet"
    ),
    "tagging_runs-youtube_videos_classes_final": partial(
        url_mapper, TAGGING_YOUTUBE_VIDEOS + "youtube_videos_classes_final.parquet"
    ),
    "tagging_runs-youtube_comments_input": partial(
        url_mapper, shared_urls.GROUP_BY_YOUTUBE_COMMENT_THREADS
    ),
    "tagging_runs-youtube_comments_pulled": partial(
        url_mapper, TAGGING_YOUTUBE_COMMENTS + "youtube_comments_pulled.parquet"
    ),
    "tagging_runs-youtube_comments_for_tagging": partial(
        url_mapper, TAGGING_YOUTUBE_COMMENTS_FOR_TAGGING + "youtube_comments_for_tagging.parquet"
    ),
    "tagging_runs-youtube_comments_final": partial(
        url_mapper, TAGGING_YOUTUBE_COMMENTS + "youtube_comments_final.parquet"
    ),
    "tagging_runs-youtube_comments_topics_final": partial(
        url_mapper, TAGGING_YOUTUBE_COMMENTS + "youtube_comments_topics_final.parquet"
    ),
    "tagging_runs-youtube_comments_classes_final": partial(
        url_mapper, TAGGING_YOUTUBE_COMMENTS + "youtube_comments_classes_final.parquet"
    ),
    # Utils
    "tagging_runs-pipeline_base": partial(url_mapper, TAGGING_PIPELINE_BASE),
    "tagging_runs-for_analyst_base": partial(url_mapper, TAGGING_FOR_ANALYST_BASE),
    # Features
    "tagging_runs-objects_for_tagging": partial(
        url_mapper, TAGGING_PIPELINE_BASE + FOR_TAGGING_SUFFIX
    ),
    "tagging_runs-key_objects": partial(url_mapper, TAGGING_PIPELINE_BASE + "key_objects.parquet"),
    "tagging_runs-objects": partial(url_mapper, TAGGING_PIPELINE_BASE + "objects.parquet"),
    "tagging_runs-all_features": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "all_features.parquet"
    ),
    "tagging_runs-sflm_unprocessed_features": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "sflm_unprocessed_features.parquet"
    ),
    # Topics
    "tagging_runs-topics": partial(url_mapper, TAGGING_PIPELINE_BASE + "topics.parquet"),
    "tagging_runs-objects_topics": partial(
        url_mapper, TAGGING_PIPELINE_BASE + "objects_topics.parquet"
    ),
    "tagging_runs-objects_topics_csv": partial(
        url_mapper, TAGGING_FOR_ANALYST_BASE + "objects_topics.csv"
    ),
    "tagging_runs-topics_csv": partial(url_mapper, TAGGING_FOR_ANALYST_BASE + "topics.csv"),
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
        url_mapper, TAGGING_PIPELINE_BASE + "language_sentiment_objects/"
    ),
    "tagging_runs-clustering": partial(url_mapper, TAGGING_PIPELINE_BASE + "clustering/"),
    "tagging_runs-clustering_dashboard": partial(
        shared_url_mapper.dashboard_url_mapper, TAGGING_PIPELINE_BASE + "clustering/"
    ),
    "tagging_runs-accounts_final": partial(
        url_mapper,
        (TAGGING_PIPELINE_BASE + "accounts_final.parquet"),
    ),
    "tagging_runs-objects_accounts_classes_final": partial(
        url_mapper,
        (TAGGING_PIPELINE_BASE + "objects_accounts_classes_final.parquet"),
    ),
}
