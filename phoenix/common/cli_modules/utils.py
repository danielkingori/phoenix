"""Cli package utilities."""
import datetime
import os
import pathlib

import click
import papermill as pm


def get_run_iso_datetime(run_iso_timestamp):
    """Get run date for cli commands."""
    return datetime.datetime.fromisoformat(run_iso_timestamp)


def relative_path(path: str, other_file: str) -> pathlib.Path:
    """Form path of the relative path from __file__'s directory."""
    return (pathlib.Path(other_file).parent.absolute() / path).absolute()


def run_notebooks(input_nb_url, output_nb_url, parameters):
    """Build input/output file paths and run notebooks."""
    create_output_dir_if_needed(output_nb_url)

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
    return None


def get_input_notebook_path(nb_phoenix_path: str):
    """Get the input notebook path relative to cwd."""
    nb = f"../../{nb_phoenix_path}"
    cwd = os.getcwd()
    return relative_path(nb, __file__).relative_to(pathlib.Path(cwd))
