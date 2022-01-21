"""Scrape Youtube."""
from typing import Any, Dict

import click

from phoenix.common import run_params
from phoenix.common.cli_modules import scrape_group, utils


@scrape_group.scrape.command("youtube")
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("endpoint", nargs=1)
@click.option(
    "--scrape_since_days",
    default=None,
    help=(
        "Number of days back from today you want to scrape."
        " Will overwrite scrape_start_date and scrape_end_date."
    ),
)
@click.option(
    "--static_youtube_channels",
    default=None,
    help=(
        "Set the static_youtube_channels URL."
        " Currently implemented for: "
        "'search_videos_from_channel_ids'"
        "'comment_threads_from_channel_ids'"
    ),
)
@click.option(
    "--max_pages",
    default=None,
    help=(
        "Set the Max pages to pull."
        " Currently implemented for: "
        "'comment_threads_from_channel_ids'"
    ),
)
def youtube(
    artifact_env,
    tenant_id,
    endpoint,
    scrape_since_days,
    static_youtube_channels,
    max_pages,
):
    """Run scrape of the youtube.

    Example commands:
    ./phoenix-cli scrape youtube production tenant channels_from_channel_ids

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    ENDPOINT: the endpoint to scrape:
        - "channels_from_channel_ids"
        - "search_videos_from_channel_ids"
        - "comment_threads_from_channel_ids"
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    # Default is empty parameters
    extra_parameters: Dict[str, Any] = {}
    # Hard coding the artifact keys so that the mypy can check it easier.
    if endpoint == "channels_from_channel_ids":
        notebook_key = "scrape/youtube/channels_from_channel_ids.ipynb"
        input_nb_url = utils.get_input_notebook_path(notebook_key)
        output_nb_url = scrape_group.get_output_notebook_url(cur_run_params, notebook_key)
    elif endpoint == "search_videos_from_channel_ids":
        notebook_key = "scrape/youtube/search_videos_from_channel_ids.ipynb"
        input_nb_url = utils.get_input_notebook_path(notebook_key)
        output_nb_url = scrape_group.get_output_notebook_url(cur_run_params, notebook_key)
    elif endpoint == "comment_threads_from_channel_ids":
        notebook_key = "scrape/youtube/comment_threads_from_channel_ids.ipynb"
        input_nb_url = utils.get_input_notebook_path(notebook_key)
        output_nb_url = scrape_group.get_output_notebook_url(cur_run_params, notebook_key)
    else:
        raise ValueError(f"Not supported endpoint: {endpoint}")

    parameters = {
        **utils.init_parameters(cur_run_params),
        **extra_parameters,
    }
    if scrape_since_days:
        parameters["SCRAPE_SINCE_DAYS"] = scrape_since_days
    if static_youtube_channels:
        parameters["STATIC_YOUTUBE_CHANNELS"] = static_youtube_channels
    if max_pages:
        parameters["MAX_PAGES"] = max_pages
    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
