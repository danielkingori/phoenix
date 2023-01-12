"""Cli package utilities."""
from typing import Any, Dict, Tuple

import datetime
import os
import pathlib

import click
import papermill as pm
import tentaclio
from dateutil.relativedelta import relativedelta

from phoenix import common
from phoenix.common import run_datetime


def init_parameters(cur_run_params: common.run_params.general.GeneralRunParams) -> Dict[str, Any]:
    """Init the parameters for the cli commands."""
    return {
        "RUN_DATETIME": cur_run_params.run_dt.to_file_safe_str(),
        "RUN_DATE": cur_run_params.run_dt.to_run_date_str(),
        "TENANT_ID": cur_run_params.tenant_config.id,
        "ARTIFACTS_ENVIRONMENT_KEY": cur_run_params.art_url_reg.environment_key,
    }


def get_run_iso_datetime(run_iso_timestamp):
    """Get run date for cli commands."""
    return datetime.datetime.fromisoformat(run_iso_timestamp)


def relative_path(path: str, other_file: str) -> pathlib.Path:
    """Form path of the relative path from __file__'s directory."""
    return (pathlib.Path(other_file).parent.absolute() / path).absolute()


def run_notebooks(input_nb_url, output_nb_url, parameters):
    """Build input/output file paths and run notebooks."""
    output_nb_url = create_output_dir_if_needed(output_nb_url)

    # Run the notebook
    click.echo(f"Running Notebook: {input_nb_url}")
    click.echo(f"Output Notebook: {output_nb_url}")
    click.echo(f"Parameters: {parameters}")
    pm.execute_notebook(input_nb_url, output_nb_url, parameters=parameters)


def create_output_dir_if_needed(output_nb):
    """If the output dir is needed then create it."""
    # This only works for local should be refactored at somepoint
    if output_nb.startswith("file:"):
        output_nb = pathlib.Path(output_nb[5:])
        output_dir = output_nb.parent
        # Make the output directory if needed
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_nb

    return output_nb


def get_input_notebook_path(nb_phoenix_path: str):
    """Get the input notebook path relative to cwd."""
    nb = f"../../{nb_phoenix_path}"
    cwd = os.getcwd()
    return relative_path(nb, __file__).relative_to(pathlib.Path(cwd))


def get_year_month_for_offset(
    run_dt: run_datetime.RunDatetime, month_offset: int
) -> Tuple[int, int]:
    """Year and month for the offset.

    Return:
        Tuple[year, month]
        e.g. (2021, 8)
    """
    offset_dt = run_dt.dt + relativedelta(months=month_offset)
    return (offset_dt.year, offset_dt.month)


def file_exists(url, silence=False):
    """Check that a file exists."""
    try:
        tentaclio.open(url, mode="r")
    except FileNotFoundError:
        message = f"File does not exists at: {url}"
        if not silence:
            raise RuntimeError(message)
        return False

    return True


def get_extra_parameters(ctx) -> Dict[str, str]:
    """Get the extra parameters from the click context."""
    return dict([item.strip("--").split("=") for item in ctx.args])
