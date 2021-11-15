"""Scrape Twitter."""
import click

from phoenix.common import run_params
from phoenix.common.cli_modules import scrape_group, utils


@scrape_group.scrape.command("tweets")
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("endpoint", nargs=1)
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
    artifact_env,
    tenant_id,
    endpoint,
    scrape_since_days,
    num_items,
):
    """Run scrape of the tweets.

    Example commands:
    ./phoenix-cli scrape tweets production tenant keyword
    ./phoenix-cli scrape tweets production tenant user

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    ENDPOINT: the endpoint to scrape "keyword", "user"
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    # Hard coding the artifact keys so that the mypy can check it easier.
    if endpoint == "user":
        input_nb_url = utils.get_input_notebook_path("scrape/twitter_user_timeline.ipynb")
        source_artifact_url = cur_run_params.art_url_reg.get_url("source-user_tweets")
        output_nb_url = cur_run_params.art_url_reg.get_url("source-twitter_user_notebook")
    elif endpoint == "keyword":
        input_nb_url = utils.get_input_notebook_path("scrape/twitter_keyword_search.ipynb")
        source_artifact_url = cur_run_params.art_url_reg.get_url("source-keyword_tweets")
        output_nb_url = cur_run_params.art_url_reg.get_url("source-twitter_keyword_notebook")
    else:
        raise ValueError(f"Not supported endpoint: {endpoint}")

    extra_parameters = {
        "QUERY_TYPE": endpoint,
        "ARTIFACT_SOURCE_TWEETS_URL": source_artifact_url,
    }
    parameters = {
        **utils.init_parameters(cur_run_params),
        **extra_parameters,
    }
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if num_items:
        parameters["NUM_ITEMS"] = num_items

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
