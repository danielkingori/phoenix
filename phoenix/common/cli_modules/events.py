"""Events CLI Interface."""
import click

from phoenix.common import run_params
from phoenix.common.cli_modules import main_group, utils


@main_group.main_group.group()
def events():
    """Events commands."""


@events.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("event_type", type=click.STRING)
@click.pass_context
def run(
    ctx,
    artifact_env,
    tenant_id,
    event_type,
):
    """Run the processing of the events data.

    Example command:
    ./phoenix-cli events run production tenant acled

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    EVENT_TYPE: acled, undp
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    parameters = utils.init_parameters(cur_run_params.run_dt, cur_run_params.art_url_reg)
    notebook_key = ""
    if event_type == "undp":
        notebook_key = "scrape/undp_events/undp_events_transform.ipynb"
        output_nb_url = cur_run_params.art_url_reg.get_url(
            "source-undp_events_notebook", parameters
        )
    elif event_type == "acled":
        notebook_key = "scrape/acled_event_transform.ipynb"
        output_nb_url = cur_run_params.art_url_reg.get_url(
            "source-acled_events_notebook", parameters
        )
    else:
        raise ValueError(f"Event type: {event_type} is not supported")

    input_nb_url = utils.get_input_notebook_path(notebook_key)

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
