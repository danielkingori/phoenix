"""Scrape CLI commands."""
import click

from phoenix.common import artifacts
from phoenix.common.cli_modules import utils


@click.group()
def scrape_cli():
    """Scrape commands."""


@scrape_cli.command()
@click.argument("run_iso_timestamp", envvar="RUN_ISO_TIMESTAMP")
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
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
    artifact_env,
    scrape_start_date,
    scrape_end_date,
):
    """Run the fb scrape script.

    Example command:
    ./phoenix-cli fb $(date --utc --iso-8601=seconds)

    RUN_ISO_TIMESTAMP:
        Is the timestamp that will mark the artifacts that are created.

    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"

    """
    run_iso_datetime = utils.get_run_iso_datetime(run_iso_timestamp)
    aur = artifacts.registry.ArtifactURLRegistry(run_iso_datetime, artifact_env)
    parameters = {
        "RUN_ISO_TIMESTAMP": aur.get_run_iso_timestamp(),
        "RUN_DATE": aur.get_run_date(),
        "ARTIFACT_SOURCE_FB_POSTS_URL": aur.get_url("source-posts"),
        "ARTIFACT_BASE_TO_PROCESS_FB_POSTS_URL": aur.get_url("base-to_process_posts"),
    }
    if scrape_start_date:
        parameters["SCRAPE_START_DATE"] = scrape_start_date

    if scrape_end_date:
        parameters["SCRAPE_END_DATE"] = scrape_end_date

    input_nb_url = utils.get_input_notebook_path("scrape/fb_post_source_api.ipynb")
    output_nb_url = aur.get_url("source-fb_post_source_api_notebook")

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)


@scrape_cli.command()
@click.argument("endpoint", nargs=1)
@click.argument("run_iso_timestamp", envvar="RUN_ISO_TIMESTAMP")
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
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
    artifact_env,
    scrape_since_days,
    num_items,
):
    """Run the twitter scrape script.

    Example commands:
    ./phoenix-cli tw keyword $(date --utc --iso-8601=seconds)
    ./phoenix-cli tw user $(date --utc --iso-8601=seconds)

    RUN_ISO_TIMESTAMP:
        Is the timestamp that will mark the artifacts that are created.
    """
    run_iso_datetime = utils.get_run_iso_datetime(run_iso_timestamp)
    aur = artifacts.registry.ArtifactURLRegistry(run_iso_datetime, artifact_env)
    # Hard coding the artifact keys so that the mypy can check it easier.
    if endpoint == "user":
        input_nb_url = utils.get_input_notebook_path("scrape/twitter_user_timeline.ipynb")
        source_artifact_url = aur.get_url("source-user_tweets")
        base_artifact_url = aur.get_url("base-to_process_user_tweets")
        output_nb_url = aur.get_url("source-twitter_user_notebook")
    elif endpoint == "keyword":
        input_nb_url = utils.get_input_notebook_path("scrape/twitter_keyword_search.ipynb")
        source_artifact_url = aur.get_url("source-keyword_tweets")
        base_artifact_url = aur.get_url("base-to_process_keyword_tweets")
        output_nb_url = aur.get_url("source-twitter_keyword_notebook")
    else:
        raise ValueError(f"Not supported endpoint: {endpoint}")

    parameters = {
        "RUN_ISO_TIMESTAMP": aur.get_run_iso_timestamp(),
        "RUN_DATE": aur.get_run_date(),
        "QUERY_TYPE": endpoint,
        "ARTIFACT_SOURCE_TWEETS_URL": source_artifact_url,
        "ARTIFACT_BASE_TWEETS_URL": base_artifact_url,
    }
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if num_items:
        parameters["NUM_ITEMS"] = num_items

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)


@scrape_cli.command()
@click.argument("run_iso_timestamp", envvar="RUN_ISO_TIMESTAMP")
def fb_comments(
    run_iso_timestamp,
):
    """Run the fb-comments parse script.

    Example command:
    ./phoenix-cli fb_comments $(date --utc --iso-8601=seconds)

    RUN_ISO_TIMESTAMP:
        Is the timestamp that will mark the artifacts that are created.
    """
    RUN_DATE = utils.get_run_iso_datetime(run_iso_timestamp)

    parameters: dict = {}

    nb_name = "fb_comments_parse.ipynb"

    utils.run_notebooks(RUN_DATE, parameters, nb_name)
