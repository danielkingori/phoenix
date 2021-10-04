"""Tagging CLI commands."""
from typing import List

import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group, utils


@main_group.main_group.group()
def tagging():
    """Tagging commands."""


@tagging.command(
    "run_phase",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("phase_number", type=click.INT)
@click.argument("object_type", type=click.STRING)
@click.argument("year_filter", type=click.INT)
@click.argument("month_filter", type=click.INT)
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.pass_context
def run_phase(
    ctx,
    phase_number,
    object_type,
    year_filter,
    month_filter,
    artifact_env,
    start_offset,
):
    """Run tagging phase.

    Example command:
    ./phoenix-cli tagging run_phase 1 facebook_posts 2021 8 production

    PHASE_NUMBER: 1 or 2
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets
    YEAR_FILTER: year. E.g. 2021
    MONTH_FILTER: month number. E.g. 8
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    _run_phase(
        ctx,
        phase_number,
        object_type,
        year_filter,
        month_filter,
        artifact_env,
        start_offset,
    )


@tagging.command(
    "run_phase_month_offset",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("phase_number", type=click.INT)
@click.argument("object_type", type=click.STRING)
@click.argument("month_offset", type=click.INT, default=0)
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.pass_context
def run_phase_month_offset(
    ctx,
    phase_number,
    object_type,
    month_offset,
    artifact_env,
    start_offset,
):
    """Run tagging of offsetting the month and year to the current month and year.

    Example command:
    ./phoenix-cli tagging run_phase_month_offset 1 facebook_posts -1 production

    PHASE_NUMBER: 1 or 2
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets
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
    _run_phase(
        ctx,
        phase_number,
        object_type,
        year_filter,
        month_filter,
        artifact_env,
        start_offset,
    )


def _run_phase(
    ctx,
    phase_number,
    object_type,
    year_filter,
    month_filter,
    artifact_env,
    start_offset,
):
    """Private function for running the tagging phase."""
    run_dt = run_datetime.create_run_datetime_now()
    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    args_parameters = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(run_dt, art_url_reg),
        **args_parameters,
        **extra_parameters,
    }

    notebooks = get_notebook_keys(phase_number, object_type)

    notebook_count = 0
    for notebook_key in notebooks:
        notebook_count = notebook_count + 1
        if notebook_count <= start_offset:
            continue
        tagging_run_notebook(notebook_key, parameters, art_url_reg)


def get_data_pull_notebook_key(object_type):
    """Get the notebook key for object type."""
    return f"tag/data_pull/{object_type}_pull_json.ipynb"


def tagging_run_notebook(
    notebook_key, parameters, art_url_reg: artifacts.registry.ArtifactURLRegistry
):
    """Run the notebook calculating the output and input notebook url."""
    input_nb_url = utils.get_input_notebook_path(notebook_key)
    output_nb_url = (
        art_url_reg.get_url("tagging_runs-output_notebook_base", parameters) + notebook_key
    )

    utils.run_notebooks(input_nb_url, output_nb_url, parameters)


def get_finalisation_notebooks(object_type) -> List[str]:
    """Get the finalisation notebooks for an object type."""
    return [
        f"tag/{object_type}_finalise.ipynb",
        f"tag/{object_type}_finalise_topics.ipynb",
    ]


def get_notebook_keys(phase_number: int, object_type) -> List[str]:
    """Get the notebooks keys for phase."""
    if phase_number == 1:
        return [
            get_data_pull_notebook_key(object_type),
            "tag/features.ipynb",
            "tag/topics.ipynb",
            "tag/tensions.ipynb",
            "tag/third_party_models/aws_async/start_sentiment.ipynb",
        ]

    if phase_number == 2:
        return [
            "tag/third_party_models/aws_async/complete_sentiment.ipynb",
        ] + get_finalisation_notebooks(object_type)

    raise ValueError(f"Unknown phase number: {phase_number}")


@tagging.command(
    "run_single",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("notebook_root_path", type=click.STRING)
@click.argument("object_type", type=click.STRING)
@click.argument("year_filter", type=click.INT)
@click.argument("month_filter", type=click.INT)
@click.argument("artifact_env", default="local", envvar="ARTIFACT_ENV")
@click.pass_context
def run_single(
    ctx,
    notebook_root_path,
    object_type,
    year_filter,
    month_filter,
    artifact_env,
):
    """Run tagging of facebook posts.

    Example command:
    ./phoenix-cli tagging run_single phoenix/tag/features.ipynb facebook_posts 2021 8 production

    NOTEBOOK_ROOT_PATH: Use path from the project root:
        e.g. phoenix/tag/features.ipynb
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets
    YEAR_FILTER: year. E.g. 2021
    MONTH_FILTER: month number. E.g. 8
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "local"
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    start_str = "phoenix/"
    if not notebook_root_path.startswith(start_str):
        raise ValueError(f"NOTEBOOK_ROOT_PATH does not start with '{start_str}'")

    notebook_key = notebook_root_path[len(start_str) :]
    run_dt = run_datetime.create_run_datetime_now()
    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    args_parameters = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(run_dt, art_url_reg),
        **args_parameters,
        **extra_parameters,
    }

    tagging_run_notebook(notebook_key, parameters, art_url_reg)
