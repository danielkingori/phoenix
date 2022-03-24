"""Init."""
from phoenix.tag.export_manual_scraping import filtering, run_params


filter_posts = filtering.filter_posts

DEFAULT_POSTS_TO_SCRAPE_COLUMNS = [
    "phoenix_post_id",
    "account_name",
    "post_created",
    "text",
    "total_interactions",
    "post_url",
    "scrape_url",
]
