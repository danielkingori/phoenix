"""Artifact keys."""
from typing import Literal


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
    # Tagging facebook
    "tagging_runs-facebook_posts_input",
    "tagging_runs-facebook_posts_pulled",
    "tagging_runs-facebook_posts_for_tagging",
]
