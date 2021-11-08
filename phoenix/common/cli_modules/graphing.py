"""Graphing CLI Interface."""
from typing import Any, Dict

import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group, tagging, utils


def get_run_config_for_graph_type(
    graph_type: str,
    art_url_reg: artifacts.registry.ArtifactURLRegistry,
    parameters: Dict[Any, Any],
):
    """Get the run config for the graph type.

    Arguments:
        graph_type (str): facebook_posts_topics, facebook_comments_topics, retweets
        art_url_reg (ArtifactURLRegistry): artifacts URL registiry to get the needed URLs
        parameters for the config (Dict): parameters for the config

    Returns:
        Dict: {
            notebook_keys: List[str],
            parameters: Dict[str, Any],
            required_artifacts: List[urls]
        }
    """
    run_dict: Dict[str, Any] = {}
    if graph_type == "facebook_posts_topics":
        run_dict["notebook_keys"] = [
            "tag/data_pull/facebook_topic_pull_graphing.ipynb",
            "tag/graphing/facebook_topic_graph.ipynb",
        ]
        run_dict["parameters"] = {"OBJECT_TYPE": "facebook_posts"}
        run_dict["required_artifacts"] = [
            art_url_reg.get_url("final-facebook_posts_topics", parameters)
        ]
    elif graph_type == "facebook_comments_topics":
        run_dict["notebook_keys"] = [
            "tag/data_pull/facebook_topic_pull_graphing.ipynb",
            "tag/graphing/facebook_topic_graph.ipynb",
        ]
        run_dict["parameters"] = {"OBJECT_TYPE": "facebook_comments"}
        run_dict["required_artifacts"] = [
            art_url_reg.get_url("final-facebook_comments_topics", parameters)
        ]
    elif graph_type == "retweets":
        run_dict["notebook_keys"] = [
            "tag/data_pull/twitter_pull_retweets.ipynb",
            "tag/graphing/twitter_retweets_graph.ipynb",
        ]
        run_dict["parameters"] = {"OBJECT_TYPE": "tweets"}
        run_dict["required_artifacts"] = []
    else:
        raise ValueError(f"Graph type: {graph_type} is not supported")

    return run_dict


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
        "This is for when the CLI is used in cron jobs so that unnecessary errors aren't created."
    ),
)
@click.pass_context
def run(
    ctx,
    graph_type,
    month_offset,
    artifact_env,
    start_offset,
    silence_no_files_to_process_exception,
):
    """Run the processing of graph data.

    Example command:
    ./phoenix-cli graphing run facebook_posts_topics -1 production

    GRAPH_TYPE: retweets, facebook_posts_topics, facebook_comments_topics
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
    init_parameters = {
        **utils.init_parameters(run_dt, art_url_reg),
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    run_config = get_run_config_for_graph_type(graph_type, art_url_reg, init_parameters)
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
    tagging._run_notebooks(run_config["notebook_keys"], parameters, art_url_reg, start_offset)
