"""Facebook Comments Pages CLI commands."""
import click

from phoenix.common import artifacts, run_datetime
from phoenix.common.cli_modules import main_group


@main_group.main_group.group()
def facebook_comments_pages():
    """facebook_comments_pages commands."""


@facebook_comments_pages.command("aws_copy_command")
@click.argument("input_directory")
@click.argument("year_filter")
@click.argument("month_filter")
@click.argument("artifact_env", default="production")
def cp_facebook_comments_pages(
    input_directory,
    year_filter,
    month_filter,
    artifact_env,
):
    """Get aws command to copy the downloaded file to s3.

    This is a not yet complete but a quick sketch to get something together.

    Example comment:
    ./phoenix-cli cp_facebook_comments_pages 2021 8 production

    INPUT_DIRECTORY:
        String of the input directory argument for the command.
        A simple solution to this is "." and then `cd` to the correct directory
        and run the printed command.
    YEAR_FILTER:
        The year that the posts that have been downloaded have been created. eg. 2021
    MONTH_FILTER:
        The month as a number that the posts that have been downloaded have been created.
        (For August) eg. 8
    ARTIFACT_ENV:
        The artifact environment that will be used. Default "production"
        Can use a valid storage URL like "s3://my-phoenix-bucket/"
    """
    run_dt = run_datetime.create_run_datetime_now()
    art_url_reg = artifacts.registry.ArtifactURLRegistry(run_dt, artifact_env)
    url_config = {
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    base_to_parse = art_url_reg.get_url("base-facebook_comments_pages_to_parse", url_config)
    click.echo(f"Command to run: aws s3 cp --recursive {input_directory} {base_to_parse}")
