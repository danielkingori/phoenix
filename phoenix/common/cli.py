"""Scrape init."""
# Importing the non used cli_modules
# So that the CLI is correctly initialised
from phoenix.common.cli_modules import (  # noqa: F401
    comments,
    events,
    facebook_comments_pages,
    graphing,
    main_group,
    scrape_facebook,
    scrape_group,
    scrape_twitter,
    tagging,
    utils,
)


if __name__ == "__main__":
    main_group.main_group()
