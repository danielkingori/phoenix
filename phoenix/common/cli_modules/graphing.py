"""Graphing CLI Interface."""

import click

from phoenix.common import run_params
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
@click.argument("object_type", type=click.STRING)
@click.argument("graph_name", type=click.STRING)
@click.argument("month_offset", type=click.INT, default=0)
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.pass_context
def run(
    ctx,
    artifact_env,
    tenant_id,
    object_type,
    graph_name,
    month_offset,
    start_offset,
):
    """Run the processing of graph data.

    Example command:
    ./phoenix-cli graphing run production TENANT_ID facebook_posts facebook_posts_commenters

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets, youtube_videos, youtube_comments
    GRAPH_NAME:
        - any_object_type_class_cooccurrence
        - facebook_posts_commenters
        - tweets_retweets
        - youtube_videos_commenters
    MONTH_OFFSET: Number of months to offset by. E.g. 0 is current month, -1 is previous month.

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    year_filter, month_filter = utils.get_year_month_for_offset(
        cur_run_params.run_dt, month_offset
    )
    args_parameters = {
        "OBJECT_TYPE": object_type,
        "GRAPH_NAME": graph_name,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    extra_parameters = utils.get_extra_parameters(ctx)
    parameters = {
        **utils.init_parameters(cur_run_params),
        **args_parameters,
        **extra_parameters,
    }
    tagging._run_notebooks(
        [f"tag/graphing/{graph_name}.ipynb"],
        parameters,
        cur_run_params.art_url_reg,
        start_offset,
    )
