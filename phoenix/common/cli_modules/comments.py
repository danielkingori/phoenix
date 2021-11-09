"""Comments CLI Interface."""
from typing import List

import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group, tagging, utils
from phoenix.scrape.fb_comment_parser import run


@main_group.main_group.group()
def comments():
    """Comments commands."""


def get_files_to_process(url_to_folder) -> List[str]:
    """Get the list of raw HTML files to process."""
    return list(run.get_files(url_to_folder))


@comments.command(
    "process_and_tag",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("month_offset", type=click.INT, default=0)
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
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
@click.pass_context
def run_phase(
    ctx,
    month_offset,
    artifact_env,
    start_offset,
    silence_no_files_to_process_exception,
):
    """Run processing and tagging of the raw comment data.

    Example command:
    ./phoenix-cli comments process_and_tag -1 production

    MONTH_OFFSET: Number of months to offset by. E.g. 0 is current month, -1 is previous month.
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    run_dt = run_datetime.create_run_datetime_now()
    year_filter, month_filter = utils.get_year_month_for_offset(run_dt, month_offset)
    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    args_parameters = {
        "OBJECT_TYPE": "facebook_comments",
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(run_dt, art_url_reg),
        **args_parameters,
        **extra_parameters,
    }

    BASE_URL_FACEBOOK_COMMENTS_PAGES_TO_PARSE = art_url_reg.get_url(
        "base-facebook_comments_pages_to_parse", parameters
    )

    files_to_process = get_files_to_process(BASE_URL_FACEBOOK_COMMENTS_PAGES_TO_PARSE)
    if len(files_to_process) < 1:
        message = (
            f"There are no files to process in folder: {BASE_URL_FACEBOOK_COMMENTS_PAGES_TO_PARSE}"
        )
        if not silence_no_files_to_process_exception:
            raise RuntimeError(message)
        click.echo(message)
        return

    click.echo("Processing files:")
    for f in files_to_process:
        click.echo(f)

    if start_offset < 1:
        tagging.tagging_run_notebook(
            "scrape/facebook_comments_pages_parse.ipynb", parameters, art_url_reg
        )

    start_offset = start_offset - 1
    tagging._run_tagging_notebooks(1, "facebook_comments", parameters, art_url_reg, start_offset)


@comments.command(
    "tag_phase_2",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("month_offset", type=click.INT, default=0)
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
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
@click.pass_context
def tag_phase_2(
    ctx,
    month_offset,
    artifact_env,
    start_offset,
    silence_no_files_to_process_exception,
):
    """Run processing and phase 2 tagging if output for phase 1 exists.

    Example command:
    ./phoenix-cli comments tag_phase_2 -1 production

    MONTH_OFFSET: Number of months to offset by. E.g. 0 is current month, -1 is previous month.
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    run_dt = run_datetime.create_run_datetime_now()
    year_filter, month_filter = utils.get_year_month_for_offset(run_dt, month_offset)
    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    args_parameters = {
        "OBJECT_TYPE": "facebook_comments",
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(run_dt, art_url_reg),
        **args_parameters,
        **extra_parameters,
    }

    TAGGING_RUNS_URL_ASYNC_JOB_GROUP = art_url_reg.get_url(
        "tagging_runs-async_job_group", parameters
    )
    if not utils.file_exists(
        TAGGING_RUNS_URL_ASYNC_JOB_GROUP, silence_no_files_to_process_exception
    ):
        message = f"There is no file from phase 1: {TAGGING_RUNS_URL_ASYNC_JOB_GROUP}"
        click.echo(message)
        return

    tagging._run_tagging_notebooks(2, "facebook_comments", parameters, art_url_reg, start_offset)
