"""Comments CLI Interface."""
import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group, tagging, utils


@main_group.main_group.group()
def comments():
    """Comments commands."""


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
@click.pass_context
def run_phase(
    ctx,
    month_offset,
    artifact_env,
    start_offset,
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

    if start_offset < 1:
        tagging.tagging_run_notebook(
            "scrape/facebook_comments_pages_parse.ipynb", parameters, art_url_reg
        )

    start_offset = start_offset - 1
    tagging._run_tagging_notebooks(1, "facebook_comments", parameters, art_url_reg, start_offset)
