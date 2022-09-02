"""Shared URLs."""

BASE_BASE = "base/"
YEAR_MONTH_FILTER_DIRS = "year_filter={YEAR_FILTER}/month_filter={MONTH_FILTER}/"
GROUP_BY_FACEBOOK_POSTS = (
    f"{BASE_BASE}grouped_by_year_month/facebook_posts/{YEAR_MONTH_FILTER_DIRS}"
)
GROUP_BY_TWEETS = f"{BASE_BASE}grouped_by_year_month/tweets/{YEAR_MONTH_FILTER_DIRS}"
GROUP_BY_FACEBOOK_COMMENTS = (
    f"{BASE_BASE}grouped_by_year_month/facebook_comments/{YEAR_MONTH_FILTER_DIRS}"
)
GROUP_BY_FACEBOOK_PAGES = (
    f"{BASE_BASE}grouped_by_year_month/facebook_posts/{YEAR_MONTH_FILTER_DIRS}"
)
TAGGING_PIPELINE_BASE = f"tagging_runs/{YEAR_MONTH_FILTER_DIRS}" + "{OBJECT_TYPE}/"
GROUP_BY_YOUTUBE_CHANNELS = (
    f"{BASE_BASE}grouped_by_year_month/youtube_channels/{YEAR_MONTH_FILTER_DIRS}"
)
GROUP_BY_YOUTUBE_SEARCH_VIDEOS = (
    f"{BASE_BASE}grouped_by_year_month/youtube_search_videos/{YEAR_MONTH_FILTER_DIRS}"
)
GROUP_BY_YOUTUBE_COMMENT_THREADS = (
    f"{BASE_BASE}grouped_by_year_month/youtube_comment_threads/{YEAR_MONTH_FILTER_DIRS}"
)

BASE_FACEBOOK_FEED = f"{BASE_BASE}facebook_feed/"
