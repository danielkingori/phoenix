"""CLI tool for phoenix."""
import click

from phoenix.common.cli_modules import facebook_comments_pages, scrape


cli = click.CommandCollection(
    sources=[scrape.scrape_cli, facebook_comments_pages.facebook_comments_pages_cli]
)

if __name__ == "__main__":
    cli()
