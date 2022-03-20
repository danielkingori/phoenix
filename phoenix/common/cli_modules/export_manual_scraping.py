"""Export Manual scraping."""
import click

from phoenix.common import run_params
from phoenix.common.cli_modules import main_group, utils


@main_group.main_group.group()
def export_manual_scraping():
    """export_manual_scraping commands."""


@export_manual_scraping.command(
    "facebook_posts",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("artifact_env")
@click.argument("tenant_id")
@click.argument("year_filter", type=click.INT)
@click.argument("month_filter", type=click.INT)
@click.option(
    "--head",
    help=("Cut the posts to scrape."),
)
@click.option(
    "--include_accounts",
    help=("Accounts to include in the posts to scrape. Format: 'account_1,account_2'"),
)
@click.option(
    "--has_topics",
    default=False,
    help=("If True posts to scrape will be filtered by has_topics True (relevant)"),
)
@click.option("--custom_prefix", help=("A custom prefix on persisted csv"))
@click.pass_context
def facebook_posts(
    ctx,
    artifact_env,
    tenant_id,
    year_filter,
    month_filter,
    head,
    include_accounts,
    has_topics,
    custom_prefix,
):
    """Run create of the labelling from pulled_data.

    Example command:
    ./phoenix-cli export_manual_scraping facebook_posts production tenant 2022 1

    ARTIFACT_ENV:
        The artifact environment that will be used.
        Can use "production" which will pick the artifact env from the env var.
        Or a valid storage URL like "s3://my-phoenix-bucket/"
    TENANT_ID: The id of the tenant to run phoenix for.

    Extra options will be added as parameters for all notebooks. E.g.
    --SOME_URL='s3://other-bucket/` will be a parameter for all notebooks.
    """
    cur_run_params = run_params.general.create(artifact_env, tenant_id)
    args_parameters = {
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
        "HEAD": head,
        "INCLUDE_ACCOUNTS": include_accounts,
        "HAS_TOPICS": has_topics,
        "CUSTOM_PREFIX": custom_prefix,
        "OBJECT_TYPE": "facebook_posts",
    }
    parameters = {
        **utils.init_parameters(cur_run_params),
        **args_parameters,
    }
    notebook_name = "export_manual_scraping/facebook_posts.ipynb"
    notebook_key = f"tag/{notebook_name}"
    input_notebook_url = utils.get_input_notebook_path(notebook_key).absolute()
    output_notebook_url = (
        cur_run_params.art_url_reg.get_url("tagging_runs-output_notebook_base", parameters)
        + notebook_name
    )
    utils.run_notebooks(
        input_notebook_url,
        output_notebook_url,
        parameters,
    )
