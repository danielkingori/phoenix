"""Graphing CLI."""
from phoenix.common.cli_modules import main_group


@main_group.main_group.group()
def graphing():
    """Graphing commands."""
