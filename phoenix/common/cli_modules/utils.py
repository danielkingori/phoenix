"""Cli package utilities."""
import datetime
import os
import pathlib

import click
import papermill as pm

from phoenix.common import artifacts


def get_run_date(run_iso_timestamp):
    """Get run date for cli commands."""
    RUN_DATE_FORMAT = "%Y-%m-%d"
    run_iso_datetime = datetime.datetime.fromisoformat(run_iso_timestamp)
    return run_iso_datetime.strftime(RUN_DATE_FORMAT)


def relative_path(path: str, other_file: str) -> pathlib.Path:
    """Form path of the relative path from __file__'s directory."""
    return (pathlib.Path(other_file).parent.absolute() / path).absolute()


def run_notebooks(run_date, parameters, nb_name):
    """Build input/output file paths and run notebooks."""
    # Build the notebook paths
    nb = f"../../scrape/{nb_name}"
    cwd = os.getcwd()
    input_nb_relative_to_cwd = relative_path(nb, __file__).relative_to(pathlib.Path(cwd))
    output_nb = pathlib.Path(f"{artifacts.urls.ARTIFACTS_PATH}/{run_date}/source_runs/{nb_name}")
    output_dir = output_nb.parent
    # Make the output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run the notebook
    click.echo(f"Running Notebook: {input_nb_relative_to_cwd}")
    click.echo(f"Output Notebook: {output_nb}")
    click.echo(f"Parameters: {parameters}")
    pm.execute_notebook(input_nb_relative_to_cwd, output_nb, parameters=parameters)
# TODO: Check execution_timeout error: "Timeout waiting for IOPub output." somewhere around NBClientEngine
