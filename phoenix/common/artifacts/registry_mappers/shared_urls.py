"""Shared URLs."""

YEAR_MONTH_FILTER_DIRS = "year_filter={YEAR_FILTER}/month_filter={MONTH_FILTER}/"
GROUP_BY_FACEBOOK_POSTS = "base/grouped_by_year_month/facebook_posts/" f"{YEAR_MONTH_FILTER_DIRS}"
GROUP_BY_TWEETS = "base/grouped_by_year_month/tweets/" f"{YEAR_MONTH_FILTER_DIRS}"
TAGGING_PIPELINE_BASE = f"tagging_runs/{YEAR_MONTH_FILTER_DIRS}" + "{OBJECT_TYPE}/"
