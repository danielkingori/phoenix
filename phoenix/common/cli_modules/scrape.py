"""Scrape CLI commands."""
import click

from phoenix.common.cli_modules import utils

@click.group()
def scrape_cli():
    """Scrape commands."""


@scrape_cli.command()
@click.argument("run_iso_timestamp", envvar="RUN_ISO_TIMESTAMP")
@click.option(
    "--scrape_start_date",
    default=None,
    help=(
        "Define a start date of the scrape data (%Y-%m-%d)."
        "Default will be one day before the date of the run iso timestampe."
    ),
)
@click.option(
    "--scrape_end_date",
    default=None,
    help=(
        "Define a end date of the scrape data (%Y-%m-%d)."
        "Default will be the date of the run iso timestamp."
    ),
)
def fb(
    run_iso_timestamp,
    scrape_start_date,
    scrape_end_date,
):
    """Run the fb scrape script.

    Example command:
    ./phoenix-cli fb $(date --utc --iso-8601=seconds)

    RUN_ISO_TIMESTAMP:
        Is the timestamp that will mark the artifacts that are created.
    """
    RUN_DATE = utils.get_run_date(run_iso_timestamp)

    parameters = {}
    if scrape_start_date:
        parameters["SCRAPE_START_DATE"] = scrape_start_date

    if scrape_end_date:
        parameters["SCRAPE_END_DATE"] = scrape_end_date

    nb_name = "fb_posts_source_api.ipynb"

    utils.run_notebooks(RUN_DATE, parameters, nb_name)



@scrape_cli.command()
@click.argument("endpoint", nargs=1)
@click.argument("run_iso_timestamp", envvar="RUN_ISO_TIMESTAMP")
@click.option(
    "--scrape_since_days",
    default=None,
    help=("Set number of days to scrape back with an integer."),
)
@click.option(
    "--num_items",
    default=None,
    help=("Maximum number of tweets to scrape." "Use 0 for no maximum."),
)
def tw(
    run_iso_timestamp,
    endpoint,
    scrape_since_days,
    num_items,
):
    """Run the twitter scrape script.

    Example commands:
    ./phoenix-cli tw keywords $(date --utc --iso-8601=seconds)
    ./phoenix-cli tw users $(date --utc --iso-8601=seconds)

    RUN_ISO_TIMESTAMP:
        Is the timestamp that will mark the artifacts that are created.
    """
    RUN_DATE = utils.get_run_date(run_iso_timestamp)

    parameters = {}
    parameters["QUERY_TYPE"] = endpoint
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if num_items:
        parameters["NUM_ITEMS"] = num_items

    if endpoint == "users":
        nb_name = "twitter_user_timeline.ipynb"
    elif endpoint == "keywords":
        nb_name = "twitter_keyword_search.ipynb"

    utils.run_notebooks(RUN_DATE, parameters, nb_name)



