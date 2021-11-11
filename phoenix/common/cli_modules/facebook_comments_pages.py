"""Facebook Comments Pages CLI commands."""
import click

from phoenix.common import run_params
from phoenix.common.cli_modules import main_group


@main_group.main_group.group()
def facebook_comments_pages():
    """facebook_comments_pages commands."""


@facebook_comments_pages.command("aws_copy_command")
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("input_directory")
@click.argument("year_filter")
@click.argument("month_filter")
def cp_facebook_comments_pages(
    artifact_env,
    tenant_id,
    input_directory,
    year_filter,
    month_filter,
):
    """Get aws command to copy the downloaded file to s3.

    This is a not yet complete but a quick sketch to get something together.

    Example comment:
    ./phoenix-cli cp_facebook_comments_pages production tenant 2021 8

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.
    INPUT_DIRECTORY:
        String of the input directory argument for the command.
        A simple solution to this is "." and then `cd` to the correct directory
        and run the printed command.
    YEAR_FILTER:
        The year that the posts that have been downloaded have been created. eg. 2021
    MONTH_FILTER:
        The month as a number that the posts that have been downloaded have been created.
        (For August) eg. 8
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    url_config = {
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }
    base_to_parse = cur_run_params.art_url_reg.get_url(
        "base-facebook_comments_pages_to_parse", url_config
    )
    click.echo(f"Command to run: aws s3 cp --recursive {input_directory} {base_to_parse}")
