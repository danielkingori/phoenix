"""SFLM CLI commands."""
from typing import Any, Dict, List

from dataclasses import dataclass

import click

from phoenix.common import run_params
from phoenix.common.cli_modules import main_group, utils


SUPPORT_INFERENCE = ["topics", "classes", "tensions", "sentiment"]


@dataclass
class SFLMPapermillRun:
    """A object to configure the run of papermill."""

    input_notebook_url: str
    output_notebook_url: str
    parameters: Dict[str, Any]


@main_group.main_group.group()
def sflm():
    """SFLM commands."""


@sflm.command(
    "create_from_labels",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("object_types", type=click.STRING, nargs=-1)
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.pass_context
def create_from_labels(
    ctx,
    artifact_env,
    tenant_id,
    object_types,
    start_offset,
):
    """Run create of the sflm from the labels.

    Example command:
    ./phoenix-cli sflm create_from_labels production tenant facebook_posts tweets

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
    sflm_papermill_runs = get_run_objects(cur_run_params, object_types, extra_parameters)
    notebook_count = 0
    for sflm_papermill_run in sflm_papermill_runs:
        notebook_count = notebook_count + 1
        if notebook_count <= start_offset:
            continue

        utils.run_notebooks(
            sflm_papermill_run.input_notebook_url,
            sflm_papermill_run.output_notebook_url,
            sflm_papermill_run.parameters,
        )


def get_run_objects(
    cur_run_params: run_params.general.GeneralRunParams,
    object_types: List[str],
    extra_parameters: Dict[str, str],
) -> List[SFLMPapermillRun]:
    """Get the run objects."""
    base_parameters = {
        **utils.init_parameters(cur_run_params),
        **extra_parameters,
    }
    sflm_papermill_runs = []
    for object_type in object_types:
        sflm_papermill_runs.append(
            single_object_type_notebook_run(
                "pull_accounts_labelling_sheet.ipynb", cur_run_params, object_type, base_parameters
            )
        )
        sflm_papermill_runs.append(
            single_object_type_notebook_run(
                "pull_objects_labelling_sheet.ipynb", cur_run_params, object_type, base_parameters
            )
        )
        sflm_papermill_runs.append(
            single_object_type_notebook_run(
                "persist_sflm_to_config.ipynb", cur_run_params, object_type, base_parameters
            )
        )

    sflm_papermill_runs.append(
        backport_sflm_config_to_sflm_config(cur_run_params, object_types, base_parameters)
    )

    return sflm_papermill_runs


@sflm.command(
    "recalculate_sflm_processed_features",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("object_types", type=click.STRING, nargs=-1)
@click.option(
    "--start_offset",
    default=0,
    help=("Start notebook from offset."),
)
@click.pass_context
def recalculate_sflm_processed_features(
    ctx,
    artifact_env,
    tenant_id,
    object_types,
    start_offset,
):
    """Recalculate the sflm processed features .

    Example command:
    ./phoenix-cli sflm recalculate_sflm_processed_features production tenant facebook_posts tweets

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
    sflm_papermill_runs = get_process_features_run_objects(
        cur_run_params, object_types, extra_parameters
    )
    notebook_count = 0
    for sflm_papermill_run in sflm_papermill_runs:
        notebook_count = notebook_count + 1
        if notebook_count <= start_offset:
            continue

        utils.run_notebooks(
            sflm_papermill_run.input_notebook_url,
            sflm_papermill_run.output_notebook_url,
            sflm_papermill_run.parameters,
        )


def get_process_features_run_objects(
    cur_run_params: run_params.general.GeneralRunParams,
    object_types: List[str],
    extra_parameters: Dict[str, str],
) -> List[SFLMPapermillRun]:
    """Get the run objects for reprocessing features for SFLM."""
    base_parameters = {
        **utils.init_parameters(cur_run_params),
        **extra_parameters,
    }
    sflm_papermill_runs = []
    for object_type in object_types:
        sflm_papermill_runs.append(
            single_object_type_notebook_run(
                "recalculate_sflm_processed_features.ipynb",
                cur_run_params,
                object_type,
                base_parameters,
            )
        )

    return sflm_papermill_runs


def single_object_type_notebook_run(
    notebook_name: str,
    cur_run_params: run_params.general.GeneralRunParams,
    object_type: str,
    base_parameters: Dict[str, str],
) -> SFLMPapermillRun:
    """Get the SFLMPapermillRun for single object type notebook."""
    args_parameters = {"OBJECT_TYPE": object_type}
    parameters = {
        **base_parameters,
        **args_parameters,
    }
    return SFLMPapermillRun(
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
    return cur_run_params.art_url_reg.get_url("sflm-output_notebook_base") + notebook_name


def backport_sflm_config_to_sflm_config(
    cur_run_params: run_params.general.GeneralRunParams,
    object_types: List[str],
    base_parameters: Dict[str, Any],
) -> SFLMPapermillRun:
    """Get the SFLMPapermillRun for backport_sflm_config_to_sflm_config.ipynb."""
    args_parameters = {"OBJECT_TYPES": list(object_types)}
    parameters = {
        **base_parameters,
        **args_parameters,
    }
    notebook_name = "backport_sflm_config_to_sfm_config.ipynb"
    return SFLMPapermillRun(
        input_notebook_url=get_input_notebook_url(notebook_name, cur_run_params),
        output_notebook_url=get_output_notebook_url(notebook_name, cur_run_params),
        parameters=parameters,
    )
