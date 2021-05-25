"""CLI tool for phoenix."""
import click

from phoenix.common.cli_modules import scrape


cli = click.CommandCollection(sources=[scrape.scrape_cli])

if __name__ == "__main__":
    cli()
