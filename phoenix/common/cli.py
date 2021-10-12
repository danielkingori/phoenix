"""Scrape init."""
# Importing the non used cli_modules
# So that the CLI is correctly initialised
from phoenix.common.cli_modules import (  # noqa: F401
    comments,
    facebook_comments_pages,
    main_group,
    scrape,
    tagging,
    utils,
)


if __name__ == "__main__":
    main_group.main_group()
