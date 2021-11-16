"""Scrape Youtube."""
from typing import Any, Dict

import click

from phoenix.common import run_params
from phoenix.common.cli_modules import scrape_group, utils


@scrape_group.scrape.command("youtube")
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("endpoint", nargs=1)
def youtube(
    artifact_env,
    tenant_id,
    endpoint,
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
        - "videos_from_channel_ids"
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    # Default is empty parameters
    extra_parameters: Dict[str, Any] = {}
    # Hard coding the artifact keys so that the mypy can check it easier.
    if endpoint == "channels_from_channel_ids":
        notebook_key = "scrape/youtube/channels_from_channel_ids.ipynb"
        input_nb_url = utils.get_input_notebook_path(notebook_key)
        output_nb_url = scrape_group.get_output_notebook_url(cur_run_params, notebook_key)
    if endpoint == "videos_from_channel_ids":
        notebook_key = "scrape/youtube/videos_from_channel_ids.ipynb"
        input_nb_url = utils.get_input_notebook_path(notebook_key)
        output_nb_url = scrape_group.get_output_notebook_url(cur_run_params, notebook_key)
    else:
        raise ValueError(f"Not supported endpoint: {endpoint}")

    parameters = {
        **utils.init_parameters(cur_run_params),
        **extra_parameters,
    }
    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
