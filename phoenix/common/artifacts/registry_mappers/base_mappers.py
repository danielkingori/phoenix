"""Base mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


GROUP_BY_FACEBOOK_COMMENT_PAGES = (
    "base/grouped_by_year_month/facebook_comments_pages/" f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
)
GROUP_BY_FACEBOOK_POST_PAGES = (
    "base/grouped_by_year_month/facebook_posts_pages/" f"{shared_urls.YEAR_MONTH_FILTER_DIRS}"
)

MAPPERS: MapperDict = {
    # Facebook Posts
    "base-grouped_by_posts": partial(
        url_mapper, shared_urls.GROUP_BY_FACEBOOK_POSTS + "posts-{RUN_DATETIME}.json"
    ),
    # Twitter Tweets
    "base-grouped_by_user_tweets": partial(
        url_mapper, shared_urls.GROUP_BY_TWEETS + "user_tweets-{RUN_DATETIME}.json"
    ),
    "base-grouped_by_keyword_tweets": partial(
        url_mapper, shared_urls.GROUP_BY_TWEETS + "keyword_tweets-{RUN_DATETIME}.json"
    ),
    # Facebook Comments
    "base-grouped_by_facebook_comments": partial(
        url_mapper,
        shared_urls.GROUP_BY_FACEBOOK_COMMENTS + "facebook_comments-{RUN_DATETIME}.json",
    ),
    "base-facebook_comments_pages_to_parse": partial(
        url_mapper, GROUP_BY_FACEBOOK_COMMENT_PAGES + "to_parse/"
    ),
    "base-facebook_comments_pages_successful_parse": partial(
        url_mapper,
        GROUP_BY_FACEBOOK_COMMENT_PAGES + "successful_parse/{RUN_DATETIME}/",
    ),
    "base-facebook_comments_pages_failed_parse": partial(
        url_mapper, GROUP_BY_FACEBOOK_COMMENT_PAGES + "failed_parse/{RUN_DATETIME}/"
    ),
    "base-facebook_comments_pages_all_failed_parse": partial(
        url_mapper, GROUP_BY_FACEBOOK_COMMENT_PAGES + "failed_parse/"
    ),
    # Facebook Posts - manual scraping
    "base-grouped_by_facebook_posts": partial(
        url_mapper,
        shared_urls.GROUP_BY_FACEBOOK_PAGES + "facebook_posts-{RUN_DATETIME}.json",
    ),
    "base-facebook_posts_pages_to_parse": partial(
        url_mapper, GROUP_BY_FACEBOOK_POST_PAGES + "to_parse/"
    ),
    "base-facebook_posts_pages_successful_parse": partial(
        url_mapper,
        GROUP_BY_FACEBOOK_POST_PAGES + "successful_parse/{RUN_DATETIME}/",
    ),
    "base-facebook_posts_pages_failed_parse": partial(
        url_mapper, GROUP_BY_FACEBOOK_POST_PAGES + "failed_parse/{RUN_DATETIME}/"
    ),
    # Acled events
    "base-acled_events_input": partial(url_mapper, shared_urls.BASE_BASE + "acled_events/"),
    "base-undp_events_input": partial(url_mapper, shared_urls.BASE_BASE + "undp_events/"),
    # YouTube
    "base-grouped_by_youtube_channels": partial(
        url_mapper,
        shared_urls.GROUP_BY_YOUTUBE_CHANNELS + "youtube_channels-{RUN_DATETIME}.json",
    ),
    "base-grouped_by_youtube_search_videos": partial(
        url_mapper,
        shared_urls.GROUP_BY_YOUTUBE_SEARCH_VIDEOS + "youtube_search_videos-{RUN_DATETIME}.json",
    ),
    "base-grouped_by_youtube_comment_threads": partial(
        url_mapper,
        shared_urls.GROUP_BY_YOUTUBE_COMMENT_THREADS
        + "youtube_comment_threads-{RUN_DATETIME}.json",
    ),
}
