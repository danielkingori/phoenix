"""Graphing CLI Interface."""
from typing import Any, Dict

import click

from phoenix.common import artifacts, run_params
from phoenix.common.cli_modules import main_group, tagging, utils


@main_group.main_group.group()
def graphing():
    """Graphing commands."""


@graphing.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("graph_type", type=click.STRING)
@click.argument("month_offset", type=click.INT, default=0)
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
def run(
    ctx,
    artifact_env,
    tenant_id,
    graph_type,
    month_offset,
    start_offset,
    silence_no_files_to_process_exception,
):
    """Run the processing of graph data.

    Example command:
    ./phoenix-cli graphing run facebook_posts_topics production -1

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    GRAPH_TYPE: retweets, facebook_posts_topics, facebook_comments_topics
    MONTH_OFFSET: Number of months to offset by. E.g. 0 is current month, -1 is previous month.

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    year_filter, month_filter = utils.get_year_month_for_offset(
        cur_run_params.run_dt, month_offset
    )
    init_parameters = {
        **utils.init_parameters(cur_run_params),
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    run_config = get_run_config_for_graph_type(
        graph_type, cur_run_params.art_url_reg, init_parameters
    )
    parameters = {
        **init_parameters,
        **run_config["parameters"],
    }
    for required_artifact in run_config["required_artifacts"]:
        message = f"Checking file required: {required_artifact}"
        click.echo(message)
        if not utils.file_exists(required_artifact, silence_no_files_to_process_exception):
            message = f"Required file not found: {required_artifact}."
            click.echo(message)
            return
    tagging._run_notebooks(
        run_config["notebook_keys"], parameters, cur_run_params.art_url_reg, start_offset
    )
