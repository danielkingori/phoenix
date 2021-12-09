"""Tagging CLI commands."""
from typing import Any, Dict, List, Optional

import click

from phoenix.common import artifacts, run_datetime, run_params
from phoenix.common.cli_modules import main_group, utils


SUPPORT_INFERENCE = ["topics", "classes", "tensions", "sentiment"]


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
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("phase_number", type=click.INT)
@click.argument("object_type", type=click.STRING)
@click.argument("year_filter", type=click.INT)
@click.argument("month_filter", type=click.INT)
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.option(
    "--include_inference",
    multiple=True,
    help=(
        "Include an inference."
        "Use this multiple times for each inference"
        f"Supported inference: {SUPPORT_INFERENCE}. \n"
        "eg. --include_inference tensions --include_inference sentiment"
    ),
)
@click.pass_context
def run_phase(
    ctx,
    artifact_env,
    tenant_id,
    phase_number,
    object_type,
    year_filter,
    month_filter,
    start_offset,
    include_inference: Optional[List[str]] = [],
):
    """Run tagging phase.

    Example command:
    ./phoenix-cli tagging run_phase production tenant 1 facebook_posts 2021 8

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    PHASE_NUMBER: 1 or 2
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets, youtube_videos, youtube_comments
    YEAR_FILTER: year. E.g. 2021
    MONTH_FILTER: month number. E.g. 8

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    _run_phase(
        ctx,
        artifact_env,
        tenant_id,
        phase_number,
        object_type,
        year_filter,
        month_filter,
        start_offset,
        include_inference,
    )


@tagging.command(
    "run_phase_month_offset",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("phase_number", type=click.INT)
@click.argument("object_type", type=click.STRING)
@click.argument("month_offset", type=click.INT, default=0)
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.option(
    "--include_inference",
    multiple=True,
    help=(
        "Include an inference."
        "Use this multiple times for each inference"
        "Supported inference: `tensions`, `sentiment`. \n"
        "eg. --include_inference tensions --include_inference sentiment"
    ),
)
@click.pass_context
def run_phase_month_offset(
    ctx,
    phase_number,
    artifact_env,
    tenant_id,
    object_type,
    month_offset,
    start_offset,
    include_inference: Optional[List[str]] = [],
):
    """Run tagging of offsetting the month and year to the current month and year.

    Example command:
    ./phoenix-cli tagging run_phase_month_offset production tenant 1 facebook_posts -1

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    PHASE_NUMBER: 1 or 2
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets
    MONTH_OFFSET: Number of months to offset by. E.g. 0 is current month, -1 is previous month.

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    run_dt = run_datetime.create_run_datetime_now()
    year_filter, month_filter = utils.get_year_month_for_offset(run_dt, month_offset)
    _run_phase(
        ctx,
        artifact_env,
        tenant_id,
        phase_number,
        object_type,
        year_filter,
        month_filter,
        start_offset,
        include_inference,
    )


def _run_phase(
    ctx,
    artifact_env,
    tenant_id,
    phase_number,
    object_type,
    year_filter,
    month_filter,
    start_offset,
    include_inference: Optional[List[str]] = [],
):
    """Private function for running the tagging phase."""
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    args_parameters = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    include_inference = validate_inferences(include_inference)
    args_parameters = append_inference_params(args_parameters, include_inference)

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(cur_run_params),
        **args_parameters,
        **extra_parameters,
    }

    return _run_tagging_notebooks(
        phase_number,
        object_type,
        parameters,
        cur_run_params.art_url_reg,
        start_offset,
        include_inference,
    )


def _run_tagging_notebooks(
    phase_number,
    object_type,
    parameters,
    art_url_reg,
    start_offset,
    include_inference: List,
):
    notebooks = get_notebook_keys(phase_number, object_type, include_inference)
    return _run_notebooks(notebooks, parameters, art_url_reg, start_offset)


def _run_notebooks(notebooks, parameters, art_url_reg, start_offset):
    """Run the tagging notebooks."""
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


def get_finalisation_notebooks(object_type, include_inference: List[str]) -> List[str]:
    """Get the finalisation notebooks for an object type."""
    nbs = [
        f"tag/{object_type}_finalise.ipynb",
    ]
    if "topics" in include_inference or "classes" in include_inference:
        nbs.append(f"tag/{object_type}_finalise_topics.ipynb")
    return nbs


def get_notebook_keys(
    phase_number: int, object_type: str, include_inference: List[str]
) -> List[str]:
    """Get the notebooks keys for phase."""
    if phase_number == 1:
        nbs = [
            get_data_pull_notebook_key(object_type),
            "tag/features.ipynb",
        ]
        if "topics" in include_inference or "classes" in include_inference:
            nbs.append("tag/topics.ipynb")
        if "tensions" in include_inference:
            nbs.append("tag/tensions.ipynb")
        if "sentiment" in include_inference:
            nbs.append("tag/third_party_models/aws_async/start_sentiment.ipynb")
        return nbs

    if phase_number == 2:
        nbs = []
        if "sentiment" in include_inference:
            nbs.append("tag/third_party_models/aws_async/complete_sentiment.ipynb")
        nbs = nbs + get_finalisation_notebooks(object_type, include_inference)
        return nbs

    raise ValueError(f"Unknown phase number: {phase_number}")


@tagging.command(
    "run_single",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("notebook_root_path", type=click.STRING)
@click.argument("object_type", type=click.STRING)
@click.argument("year_filter", type=click.INT)
@click.argument("month_filter", type=click.INT)
@click.pass_context
def run_single(
    ctx,
    artifact_env,
    tenant_id,
    notebook_root_path,
    object_type,
    year_filter,
    month_filter,
):
    """Run tagging of facebook posts.

    Example command:
    ./phoenix-cli tagging run_single \
            production tenant phoenix/tag/features.ipynb facebook_posts 2021 8

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    NOTEBOOK_ROOT_PATH: Use path from the project root:
        e.g. phoenix/tag/features.ipynb
    OBJECT_TYPE: facebook_posts, facebook_comments, tweets
    YEAR_FILTER: year. E.g. 2021
    MONTH_FILTER: month number. E.g. 8
    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    start_str = "phoenix/"
    if not notebook_root_path.startswith(start_str):
        raise ValueError(f"NOTEBOOK_ROOT_PATH does not start with '{start_str}'")

    notebook_key = notebook_root_path[len(start_str) :]
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    args_parameters = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    extra_parameters = dict([item.strip("--").split("=") for item in ctx.args])
    parameters = {
        **utils.init_parameters(cur_run_params),
        **args_parameters,
        **extra_parameters,
    }

    tagging_run_notebook(notebook_key, parameters, cur_run_params.art_url_reg)


def append_inference_params(notebook_parameters: Dict[str, Any], include_inference: List[str]):
    """Append the Analysis parameters to the notebook parameters."""
    if "classes" in include_inference:
        notebook_parameters["RENAME_TOPIC_TO_CLASS"] = True

    if "tensions" in include_inference:
        notebook_parameters["INCLUDE_OBJECTS_TENSIONS"] = True
    else:
        notebook_parameters["INCLUDE_OBJECTS_TENSIONS"] = False

    if "sentiment" in include_inference:
        notebook_parameters["INCLUDE_SENTIMENT"] = True
    else:
        notebook_parameters["INCLUDE_SENTIMENT"] = False

    return notebook_parameters


def validate_inferences(include_inference: Optional[List[str]] = []) -> List[str]:
    """Validate the include_inference."""
    if not include_inference:
        return []
    if len(include_inference) == 0:
        return []
    difference = list(set(include_inference) - set(SUPPORT_INFERENCE))
    if len(difference) != 0:
        raise RuntimeError(
            "Unsupported inference: {difference}." "Please use one of {SUPPORT_INFERENCE}"
        )
    return include_inference
