"""Labelling CLI commands."""
from typing import Any, Dict, List

from dataclasses import dataclass

import click

from phoenix.common import run_params
from phoenix.common.cli_modules import main_group, utils


@dataclass
class LabellingPapermillRun:
    """A object to configure the run of papermill."""

    input_notebook_url: str
    output_notebook_url: str
    parameters: Dict[str, Any]


@main_group.main_group.group()
def labelling():
    """Labelling commands."""


@labelling.command(
    "update_labelling_sheets",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("year_filter", type=click.INT)
@click.argument("month_filter", type=click.INT)
@click.argument("object_types", type=click.STRING, nargs=-1)
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.option(
    "--goal_number_object_rows",
    default=10000,
    help=("Goal number of rows to have in objects labelling sheets."),
)
@click.pass_context
def update_labelling_sheets(
    ctx,
    artifact_env,
    tenant_id,
    year_filter,
    month_filter,
    object_types,
    start_offset,
    goal_number_object_rows,
):
    """Run create of the labelling from pulled_data.

    Example command:
    ./phoenix-cli labelling update_labelling_sheets production tenant 2022 1 facebook_posts tweets

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    OBJECT_TYPES: facebook_posts, facebook_comments, tweets, youtube_videos, youtube_comments
        Allowed multiple

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    extra_parameters = utils.get_extra_parameters(ctx)
    args_parameters = {
        "GOAL_NUM_ROWS": goal_number_object_rows,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    labelling_papermill_runs = get_run_objects(
        cur_run_params,
        object_types,
        args_parameters,
        extra_parameters,
    )
    notebook_count = 0
    for labelling_papermill_run in labelling_papermill_runs:
        notebook_count = notebook_count + 1
        if notebook_count <= start_offset:
            continue

        utils.run_notebooks(
            labelling_papermill_run.input_notebook_url,
            labelling_papermill_run.output_notebook_url,
            labelling_papermill_run.parameters,
        )


def get_run_objects(
    cur_run_params: run_params.general.GeneralRunParams,
    object_types: List[str],
    num_rows_parameters: Dict[str, str],
    extra_parameters: Dict[str, str],
) -> List[LabellingPapermillRun]:
    """Get the run objects."""
    base_parameters = {
        **utils.init_parameters(cur_run_params),
        **num_rows_parameters,
        **extra_parameters,
    }
    labelling_papermill_runs = []
    for object_type in object_types:
        labelling_papermill_runs.append(
            single_object_type_notebook_run(
                "push_account_labelling_sheet.ipynb", cur_run_params, object_type, base_parameters
            )
        )
        labelling_papermill_runs.append(
            single_object_type_notebook_run(
                "push_object_labelling_sheet.ipynb", cur_run_params, object_type, base_parameters
            )
        )

    return labelling_papermill_runs


def single_object_type_notebook_run(
    notebook_name: str,
    cur_run_params: run_params.general.GeneralRunParams,
    object_type: str,
    base_parameters: Dict[str, str],
) -> LabellingPapermillRun:
    """Get the LabellingPapermillRun for single object type notebook."""
    args_parameters = {"OBJECT_TYPE": object_type}
    parameters = {
        **base_parameters,
        **args_parameters,
    }
    return LabellingPapermillRun(
        input_notebook_url=get_input_notebook_url(notebook_name, cur_run_params),
        output_notebook_url=get_output_notebook_url(
            f"{object_type}-{notebook_name}", cur_run_params
        ),
        parameters=parameters,
    )


def get_input_notebook_url(
    notebook_name: str,
    cur_run_params: run_params.general.GeneralRunParams,
) -> str:
    """Get the input url for the notebook."""
    notebook_key = f"tag/labelling/{notebook_name}"
    return utils.get_input_notebook_path(notebook_key).absolute()


def get_output_notebook_url(
    notebook_name: str,
    cur_run_params: run_params.general.GeneralRunParams,
) -> str:
    """Get the output url for the notebook."""
    return cur_run_params.art_url_reg.get_url("labelling-output_notebook_base") + notebook_name
