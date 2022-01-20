"""Scrape init."""
# Importing the non used cli_modules
# So that the CLI is correctly initialised
from phoenix.common.cli_modules import (  # noqa: F401
    comments,
    events,
    facebook_comments_pages,
    graphing,
    labelling,
    main_group,
    scrape_facebook,
    scrape_group,
    scrape_twitter,
    scrape_youtube,
    sflm,
    tagging,
    utils,
)


if __name__ == "__main__":
    main_group.main_group()
