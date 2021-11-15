"""Scrape CLI commands."""
from phoenix.common.cli_modules import main_group


@main_group.main_group.group()
def scrape():
    """Scrape commands."""
