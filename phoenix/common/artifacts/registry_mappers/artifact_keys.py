"""Artifact keys."""
from typing import Literal


ArtifactKey = Literal[
    # Legacy
    "legacy-language_sentiment_objects",
    # static
    "static-twitter_users",
    "static-twitter_keywords",
    "static-custom_models_tension_classifier_base",
    "static-custom_models_tension_classifier_data",
    "static-youtube_channels",
    "source-notebooks_base",
    # Legacy static mapper
    "static-legacy-sfm-config",
    # Acled Events
    "source-acled_events_notebook",
    "base-acled_events_input",
    "final-acled_events",
    # UNDP Events
    "source-undp_events_notebook",
    "base-undp_events_input",
    "final-undp_events",
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
    # Facebook Comments
    "source-facebook_comments",
    "base-facebook_comments_pages_to_parse",
    "base-facebook_comments_pages_successful_parse",
    "base-facebook_comments_pages_failed_parse",
    "base-facebook_comments_pages_all_failed_parse",
    "base-grouped_by_facebook_comments",
    # Facebook Posts - manual scraping
    "source-facebook_posts",
    "base-facebook_posts_pages_to_parse",
    "base-facebook_posts_pages_successful_parse",
    "base-facebook_posts_pages_failed_parse",
    "base-grouped_by_facebook_posts",
    # Youtube
    "source-youtube_channels_from_channel_ids",
    "base-grouped_by_youtube_channels",
    "source-youtube_search_videos_from_channel_ids",
    "base-grouped_by_youtube_search_videos",
    "source-youtube_comment_threads_from_channel_ids",
    "base-grouped_by_youtube_comment_threads",
    # Tagging notebooks
    "tagging_runs-output_notebook_base",
    # Tagging facebook
    "tagging_runs-facebook_posts_input",
    "tagging_runs-facebook_posts_pulled",
    "tagging_runs-facebook_posts_for_tagging",
    "tagging_runs-facebook_posts_to_scrape_dashboard",
    "tagging_runs-facebook_posts_topics_final",
    "tagging_runs-facebook_posts_classes_final",
    "tagging_runs-custom_facebook_posts_to_scrape",
    # Tagging tweets
    "tagging_runs-tweets_input",
    "tagging_runs-tweets_pulled",
    "tagging_runs-tweets_for_tagging",
    "tagging_runs-tweets_final",
    "tagging_runs-tweets_topics_final",
    "tagging_runs-tweets_classes_final",
    # Tagging facebook comments
    "tagging_runs-facebook_comments_input",
    "tagging_runs-facebook_comments_pulled",
    "tagging_runs-facebook_comments_for_tagging",
    "tagging_runs-facebook_comments_final",
    "tagging_runs-facebook_comments_topics_final",
    "tagging_runs-facebook_comments_classes_final",
    # Tagging Youtube
    "tagging_runs-youtube_videos_input",
    "tagging_runs-youtube_videos_pulled",
    "tagging_runs-youtube_videos_for_tagging",
    "tagging_runs-youtube_videos_final",
    "tagging_runs-youtube_videos_topics_final",
    "tagging_runs-youtube_videos_classes_final",
    "tagging_runs-youtube_comments_input",
    "tagging_runs-youtube_comments_pulled",
    "tagging_runs-youtube_comments_for_tagging",
    "tagging_runs-youtube_comments_final",
    "tagging_runs-youtube_comments_topics_final",
    "tagging_runs-youtube_comments_classes_final",
    # Tagging Pipeline
    "tagging_runs-pipeline_base",
    "tagging_runs-for_analyst_base",
    "tagging_runs-objects_for_tagging",
    "tagging_runs-key_objects",
    "tagging_runs-objects",
    "tagging_runs-all_features",
    "tagging_runs-sflm_unprocessed_features",
    "tagging_runs-topics",
    "tagging_runs-objects_topics",
    "tagging_runs-objects_topics_csv",
    "tagging_runs-topics_csv",
    "tagging_runs-objects_tensions",
    "tagging_runs-async_job_group",
    "tagging_runs-comprehend_base",
    "tagging_runs-language_sentiment_objects",
    "tagging_runs-facebook_posts_final",
    "tagging_runs-facebook_posts_to_scrape",
    # Tagging accounts
    "tagging_runs-accounts_final",
    "tagging_runs-objects_accounts_classes_final",
    # Labelling
    "labelling-output_notebook_base",
    # Clustering
    "tagging_runs-clustering",
    "tagging_runs-clustering_dashboard",
    # Single Feature to Label Mapping
    "sflm-single_object_type",
    "sflm-account-object_type",
    "sflm-output_notebook_base",
    # Graphing
    "graphing-edges",
    "graphing-nodes",
    "graphing-graphistry-redirect_html",
    # Legacy graphing below this comment
    "graphing_runs-retweet_pulled",
    "graphing_runs-retweet_input",
    "graphing_runs-retweet_output_graph",
    "graphing_runs-retweet_dashboard_graph",
    "graphing_runs-facebook_topics_graph_pulled",
    "graphing_runs-facebook_topics_output_graph",
    "graphing_runs-facebook_topics_dashboard_graph",
    # Final
    "final-facebook_posts",
    "final-facebook_posts_topics",
    "final-facebook_posts_classes",
    "final-tweets",
    "final-tweets_topics",
    "final-tweets_classes",
    "final-facebook_comments",
    "final-facebook_comments_topics",
    "final-facebook_comments_classes",
    "final-youtube_videos",
    "final-youtube_videos_topics",
    "final-youtube_videos_classes",
    "final-youtube_comments",
    "final-youtube_comments_topics",
    "final-youtube_comments_classes",
    "final-accounts",
    "final-objects_accounts_classes",
]
