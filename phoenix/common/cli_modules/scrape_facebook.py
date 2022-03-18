"""Scrape Facebook."""
import click

from phoenix.common import run_params
from phoenix.common.cli_modules import scrape_group, utils


@scrape_group.scrape.command("facebook_posts")
@click.argument("artifact_env")
@click.argument("tenant_id")
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
    "--crowdtangle_list_ids",
    default=None,
    help=("A list of CrowdTangle to override the one set in the tenant." " Format: 'id1,id2'"),
)
def fb(
    artifact_env,
    tenant_id,
    scrape_since_days,
    scrape_start_date,
    scrape_end_date,
    crowdtangle_list_ids,
):
    """Run scrape of facebook posts.

    Example command:
    ./phoenix-cli scrape facebook_posts production tenant

    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    extra_parameters = {
        "ARTIFACT_SOURCE_FB_POSTS_URL": cur_run_params.art_url_reg.get_url("source-posts"),
    }
    parameters = {
        **utils.init_parameters(cur_run_params),
        **extra_parameters,
    }
    if scrape_since_days:
        parameters["SINCE_DAYS"] = scrape_since_days

    if scrape_start_date:
        parameters["SCRAPE_START_DATE"] = scrape_start_date

    if scrape_end_date:
        parameters["SCRAPE_END_DATE"] = scrape_end_date

    if crowdtangle_list_ids:
        parameters["CROWDTANGLE_LIST_IDS"] = crowdtangle_list_ids

    input_nb_url = utils.get_input_notebook_path("scrape/fb_post_source_api.ipynb")
    output_nb_url = cur_run_params.art_url_reg.get_url("source-fb_post_source_api_notebook")

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
