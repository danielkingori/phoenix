"""Events CLI Interface."""
import click

from phoenix.common import artifacts, run_datetime
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
@click.argument("event_type", type=click.STRING)
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.pass_context
def run(
    ctx,
    event_type,
    artifact_env,
):
    """Run the processing of the events data.

    Example command:
    ./phoenix-cli events run acled production

    EVENT_TYPE: acled, undp
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    """
    run_dt = run_datetime.create_run_datetime_now()
    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    parameters = utils.init_parameters(run_dt, art_url_reg)
    notebook_key = ""
    if event_type == "undp":
        notebook_key = "scrape/undp_events/undp_events_transform.ipynb"
        output_nb_url = art_url_reg.get_url("source-undp_events_notebook", parameters)
    elif event_type == "acled":
        notebook_key = "scrape/acled_event_transform.ipynb"
        output_nb_url = art_url_reg.get_url("source-acled_events_notebook", parameters)
    else:
        raise ValueError(f"Event type: {event_type} is not supported")

    input_nb_url = utils.get_input_notebook_path(notebook_key)

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)
