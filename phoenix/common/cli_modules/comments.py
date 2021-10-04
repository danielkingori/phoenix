"""Comments CLI Interface."""
from phoenix.common.cli_modules import main_group


@main_group.main_group.group()
def comments():
    """Comments commands."""
