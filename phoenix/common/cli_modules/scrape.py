"""Scrape CLI commands."""
import datetime
import os
import pathlib

import click
import papermill as pm

from phoenix.common import artifacts


@click.group()
def scrape_cli():
    """Scrape commands."""


@scrape_cli.command()
@click.argument("run_iso_timestamp", envvar="RUN_ISO_TIMESTAMP")
@click.option(
    "--scrape_start_date",
    default=None,
    help=(
        "Define a start date of the scrape data (%Y-%m-%d)."
        "Default will be one day before the date of the run iso timestampe."
    ),
)
@click.option(
    "--scrape_end_date",
    default=None,
    help=(
        "Define a end date of the scrape data (%Y-%m-%d)."
        "Default will be the date of the run iso timestamp."
    ),
)
def fb(
    run_iso_timestamp,
    scrape_start_date,
    scrape_end_date,
):
    """Run the fb scrape script.

    RUN_ISO_TIMESTAMP:
        Is the timestamp that will mark the artifacts that are created.
    """
    RUN_DATE_FORMAT = "%Y-%m-%d"
    run_iso_datetime = datetime.datetime.fromisoformat(run_iso_timestamp)
    RUN_DATE = run_iso_datetime.strftime(RUN_DATE_FORMAT)
    parameters = {}
    if scrape_start_date:
        parameters["SCRAPE_START_DATE"] = scrape_start_date

    if scrape_end_date:
        parameters["SCRAPE_END_DATE"] = scrape_end_date

    nb_name = "fb_posts_source_api.ipynb"
    nb = f"../../scrape/{nb_name}"
    # Build the notebook paths
    cwd = os.getcwd()
    input_nb_relative_to_cwd = relative_path(nb, __file__).relative_to(pathlib.Path(cwd))
    output_nb = pathlib.Path(f"{artifacts.urls.ARTIFACTS_PATH}/{RUN_DATE}/source_runs/{nb_name}")
    output_dir = output_nb.parent
    # Make the output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run the notebook
    click.echo(f"Running Notebook: {input_nb_relative_to_cwd}")
    click.echo(f"Output Notebook: {output_nb}")
    click.echo(f"Parameters: {parameters}")
    pm.execute_notebook(input_nb_relative_to_cwd, output_nb, parameters=parameters)


def relative_path(path: str, other_file: str) -> pathlib.Path:
    """Form path of the relative path from __file__'s directory."""
    return (pathlib.Path(other_file).parent.absolute() / path).absolute()
