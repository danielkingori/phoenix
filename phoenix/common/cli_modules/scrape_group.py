"""Scrape CLI commands."""
from phoenix.common.cli_modules import main_group
from phoenix.common.run_params import general


@main_group.main_group.group()
def scrape():
    """Scrape commands."""


def get_output_notebook_url(cur_run_params: general.GeneralRunParams, notebook_key: str) -> str:
    """Get the output notebook URL for that notebook key."""
    return cur_run_params.art_url_reg.get_url("source-notebooks_base") + notebook_key
