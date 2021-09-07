"""Scrape CLI commands."""
import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import utils


@click.group()
def scrape_cli():
    """Scrape commands."""


@scrape_cli.command()
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.option(
    "--scrape_start_date",
    default=None,
    help=("Define a start date of the scrape data (%Y-%m-%d)." "Default will be set in notebook."),
)
@click.option(
    "--scrape_end_date",
    default=None,
    help=(
        "Define a end date of the scrape data (%Y-%m-%d)." "Default will be set in the notebook."
    ),
)
def fb(
    artifact_env,
    scrape_start_date,
    scrape_end_date,
):
    """Run the fb scrape script.

    Example command:
    ./phoenix-cli fb

    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" or a valid storage URL like "s3://my-phoenix-bucket/"
    """
    run_dt = run_datetime.create_run_datetime_now()
    aur = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    parameters = {
        "RUN_DATETIME": run_dt.to_file_safe_str(),
        "RUN_DATE": run_dt.to_run_date_str(),
        "ARTIFACTS_ENVIRONMENT_KEY": artifact_env,
        "ARTIFACT_SOURCE_FB_POSTS_URL": aur.get_url("source-posts"),
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
    endpoint,
    artifact_env,
    scrape_since_days,
    num_items,
):
    """Run the twitter scrape script.

    Example commands:
    ./phoenix-cli tw keyword
    ./phoenix-cli tw user

    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" or a valid storage URL like "s3://my-phoenix-bucket/"
    """
    run_dt = run_datetime.create_run_datetime_now()
    aur = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    # Hard coding the artifact keys so that the mypy can check it easier.
    if endpoint == "user":
        input_nb_url = utils.get_input_notebook_path("scrape/twitter_user_timeline.ipynb")
        source_artifact_url = aur.get_url("source-user_tweets")
        output_nb_url = aur.get_url("source-twitter_user_notebook")
    elif endpoint == "keyword":
        input_nb_url = utils.get_input_notebook_path("scrape/twitter_keyword_search.ipynb")
        source_artifact_url = aur.get_url("source-keyword_tweets")
        output_nb_url = aur.get_url("source-twitter_keyword_notebook")
    else:
        raise ValueError(f"Not supported endpoint: {endpoint}")

    parameters = {
        "ARTIFACTS_ENVIRONMENT_KEY": artifact_env,
        "RUN_DATETIME": run_dt.to_file_safe_str(),
        "RUN_DATE": run_dt.to_run_date_str(),
        "QUERY_TYPE": endpoint,
        "ARTIFACT_SOURCE_TWEETS_URL": source_artifact_url,
    }
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if num_items:
        parameters["NUM_ITEMS"] = num_items

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)


@scrape_cli.command()
def fb_comments():
    """Run the fb-comments parse script.

    Example command:
    ./phoenix-cli fb_comments

    """
    run_dt = run_datetime.create_run_datetime_now()
    RUN_DATE = run_dt.to_run_date_str()

    parameters: dict = {}

    nb_name = "fb_comments_parse.ipynb"

    utils.run_notebooks(RUN_DATE, parameters, nb_name)
