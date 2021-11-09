"""Scrape CLI commands."""
import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group, utils


@main_group.main_group.group()
def scrape():
    """Scrape commands."""


@scrape.command("facebook_posts")
@click.argument("tenant_id")
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.option(
    "--scrape_since_days",
    default=None,
    help=(
        "Number of days back from today you want to scrape."
        " Will overwrite scrape_start_date and scrape_end_date."
    ),
)
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
@click.option(
    "--scrape_list_id",
    default=None,
    help=(
        "Crowdtangle list id that should be scraped."
        " If not provided the enviroment variable CROWDTANGLE_SCRAPE_LIST_ID should set."
        " If multiple crowdtangle lists need to be scraped, use commas without space, e.g. id1,id2"
    ),
)
def fb(
    tenant_id,
    artifact_env,
    scrape_since_days,
    scrape_start_date,
    scrape_end_date,
    scrape_list_id,
):
    """Run scrape of facebook posts.

    Example command:
    ./phoenix-cli scrape facebook_posts tenant_id production

    TENANT_ID: ID of the tenant of phoenix
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    """
    run_dt = run_datetime.create_run_datetime_now()
    aur = artifacts.registry.ArtifactURLRegistry(tenant_id, run_dt, artifact_env)
    extra_parameters = {
        "ARTIFACT_SOURCE_FB_POSTS_URL": aur.get_url("source-posts"),
    }
    parameters = {**utils.init_parameters(run_dt, aur), **extra_parameters}
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if scrape_start_date:
        parameters["SCRAPE_START_DATE"] = scrape_start_date

    if scrape_end_date:
        parameters["SCRAPE_END_DATE"] = scrape_end_date

    if scrape_list_id:
        parameters["SCRAPE_LIST_ID"] = scrape_list_id

    input_nb_url = utils.get_input_notebook_path("scrape/fb_post_source_api.ipynb")
    output_nb_url = aur.get_url("source-fb_post_source_api_notebook")

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)


@scrape.command("tweets")
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
    """Run scrape of the tweets.

    Example commands:
    ./phoenix-cli scrape tweets keyword production
    ./phoenix-cli scrape tweets user production

    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
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

    extra_parameters = {
        "QUERY_TYPE": endpoint,
        "ARTIFACT_SOURCE_TWEETS_URL": source_artifact_url,
    }
    parameters = {**utils.init_parameters(run_dt, aur), **extra_parameters}
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if num_items:
        parameters["NUM_ITEMS"] = num_items

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
