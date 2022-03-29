"""Scrape Facebook."""
import click

from phoenix.common import run_params
from phoenix.common.cli_modules import comments, scrape_group, tagging, utils


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
@click.option(
    "--process_manually_collected_html_files",
    is_flag=True,
    help=(
        (
            "Process the manually collected html files of facebook pages into the CrowdTangle "
            "response format, reading for 'tagging'."
        )
    ),
)
@click.option(
    "--max_manually_collected_html_files_to_process",
    type=click.INT,
    help="Max number of files to process",
)
@click.option(
    "--silence_no_files_to_process_exception",
    default=False,
    type=click.BOOL,
    help=(
        "Silence no files to process exception\n"
        "This is for when the CLI is used in cron jobs so that unnecessary errors aren't created."
    ),
)
@click.argument("month_offset", type=click.INT, default=0)
def fb(
    artifact_env,
    tenant_id,
    scrape_since_days,
    scrape_start_date,
    scrape_end_date,
    crowdtangle_list_ids,
    process_manually_collected_html_files,
    max_manually_collected_html_files_to_process,
    silence_no_files_to_process_exception,
    month_offset,
):
    """Run scrape of facebook posts.

    Example command:
    ./phoenix-cli scrape facebook_posts production tenant

    Or, to process posts from manually downloaded html files into CrowdTangle "scrapes":
    ./phoenix-cli scrape facebook_posts production tenant --process_manually_collected_html_files


    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    year_filter, month_filter = utils.get_year_month_for_offset(
        cur_run_params.run_dt, month_offset
    )
    extra_parameters = {
        "ARTIFACT_SOURCE_FB_POSTS_URL": cur_run_params.art_url_reg.get_url("source-posts"),
        "OBJECT_TYPE": "facebook_posts",
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
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

    if max_manually_collected_html_files_to_process:
        parameters["MAX_FILES_TO_PROCESS"] = max_manually_collected_html_files_to_process

    if process_manually_collected_html_files:
        BASE_URL_FACEBOOK_POSTS_PAGES_TO_PARSE = cur_run_params.art_url_reg.get_url(
            "base-facebook_posts_pages_to_parse", parameters
        )

        files_to_process = comments.get_files_to_process(BASE_URL_FACEBOOK_POSTS_PAGES_TO_PARSE)
        if len(files_to_process) < 1:
            message = (
                "There are no files to process in folder: "
                f"{BASE_URL_FACEBOOK_POSTS_PAGES_TO_PARSE}"
            )
            if not silence_no_files_to_process_exception:
                raise RuntimeError(message)
            click.echo(message)
            return

        click.echo("Processing files:")
        for f in files_to_process:
            click.echo(f)
            tagging.tagging_run_notebook(
                "scrape/facebook_posts_pages_parse.ipynb", parameters, cur_run_params.art_url_reg
            )
    else:
        input_nb_url = utils.get_input_notebook_path("scrape/fb_post_source_api.ipynb")
        output_nb_url = cur_run_params.art_url_reg.get_url("source-fb_post_source_api_notebook")

        utils.run_notebooks(input_nb_url, output_nb_url, parameters)
