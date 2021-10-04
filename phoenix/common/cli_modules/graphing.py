"""Graphing CLI."""
import click
import tentaclio

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group, tagging, utils


@main_group.main_group.group()
def graphing():
    """Graphing commands."""


@graphing.command(
    "topic",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("graph_type", type=click.STRING)
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
        "This is for when the CLI is used in cron jobs so that unnecessary errors are created."
    ),
)
@click.pass_context
def topic(
    ctx,
    graph_type,
    month_offset,
    artifact_env,
    start_offset,
    silence_no_files_to_process_exception,
):
    """Run processing for topic graph.

    Example command:
    ./phoenix-cli graphing topic facebook_posts -1 production

    GRAPH_TYPE: The graph type you want to run the pipeline for:
        "facebook_posts",
        "facebook_comments",
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
        "OBJECT_TYPE": graph_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(run_dt, art_url_reg),
        **args_parameters,
        **extra_parameters,
    }

    GRAPHING_RUNS_URL_FACEBOOK_TOPICS_GRAPH_PULLED = art_url_reg.get_url(
        "graphing_runs-facebook_topics_graph_pulled", parameters
    )
    try:
        tentaclio.open(GRAPHING_RUNS_URL_FACEBOOK_TOPICS_GRAPH_PULLED, mode="r")
    except FileNotFoundError:
        message = (
            "There is no file from phase tagging pipeline:"
            f" {GRAPHING_RUNS_URL_FACEBOOK_TOPICS_GRAPH_PULLED}"
        )
        if not silence_no_files_to_process_exception:
            raise RuntimeError(message)
        click.echo(message)
        return

    notebooks = [
        "tag/data_pull/facebook_topic_pull_graphing.ipynb",
        "tag/graphing/facebook_topic_graph.ipynb"
    ]
    tagging.tagging_run_notebooks(notebooks, parameters, art_url_reg, start_offset)
