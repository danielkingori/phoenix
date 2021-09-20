"""Artifact keys."""
from typing import Literal


ArtifactKey = Literal[
    # static
    "static-twitter_users",
    "static-custom_models_tension_classifier_base",
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
    "base-grouped_by_facebook_comments",
    # Tagging facebook
    "tagging_runs-facebook_posts_input",
    "tagging_runs-facebook_posts_pulled",
    "tagging_runs-facebook_posts_for_tagging",
    "tagging_runs-facebook_posts_topics_final",
    # Tagging tweets
    "tagging_runs-tweets_input",
    "tagging_runs-tweets_pulled",
    "tagging_runs-tweets_for_tagging",
    "tagging_runs-tweets_final",
    "tagging_runs-tweets_topics_final",
    # Tagging tweets
    "tagging_runs-facebook_comments_input",
    "tagging_runs-facebook_comments_pulled",
    "tagging_runs-facebook_comments_for_tagging",
    "tagging_runs-facebook_comments_final",
    "tagging_runs-facebook_comments_topics_final",
    # Tagging Pipeline
    "tagging_runs-pipeline_base",
    "tagging_runs-for_analyst_base",
    "tagging_runs-features_for_tagging",
    "tagging_runs-key_objects",
    "tagging_runs-objects",
    "tagging_runs-all_features",
    "tagging_runs-topics",
    "tagging_runs-objects_topics_csv",
    "tagging_runs-topics_csv",
    "tagging_runs-objects_tensions",
    "tagging_runs-async_job_group",
    "tagging_runs-comprehend_base",
    "tagging_runs-language_sentiment_objects",
    "tagging_runs-facebook_posts_final",
    "tagging_runs-facebook_posts_to_scrape",
    # Graphing
    "graphing_runs-retweet_pulled",
    "graphing_runs-retweet_input",
    "graphing_runs-retweet_output_graph",
    "graphing_runs-retweet_dashboard_graph",
    "graphing_runs-facebook_topics_graph_pulled",
    # Final
    "final-facebook_posts",
    "final-facebook_posts_topics",
    "final-tweets",
    "final-tweets_topics",
    "final-facebook_comments",
    "final-facebook_comments_topics",
]
