"""General utilities."""
import logging
import pathlib
import sys

import matplotlib.pyplot as plt
import pandas as pd


def setup_notebook_logging(level=logging.INFO):
    """Set up logging to be output within a notebook.

    Can be called at the start of a notebook to make the logging from modules output to cell
    outputs.
    """
    FORMAT = "LOG:%(asctime)s - pid:%(process)d - [%(pathname)s:%(lineno)d] - %(name)s - %(levelname)s - %(message)s"  # noqa
    logging.basicConfig(stream=sys.stdout, format=FORMAT, level=level)
    logger = logging.getLogger()
    logger.info(
        f"Outputting logs within notebook enabled. Set level:{logging.getLevelName(level)}."
    )


def setup_notebook_matplotlib_config():
    """Set up good matplotlib config applied to all plots.

    Individual plots can specifically override these settings.
    Can be called at the start of a notebook to apply config to rest of notebook.
    """
    plt.rcParams["figure.figsize"] = [20, 8]  # wide screen
    plt.style.use("ggplot")


def setup_notebook_pandas_config():
    """Sets a good pandas display config, primarily not truncating rows in dataframe outputs.

    Affects calls like:
    >>> from IPython.display import display
    >>> display(df)
    Can be called at the start of a notebook to apply config to rest of notebook.
    """
    pd.options.display.max_rows = 100
    pd.options.display.max_columns = 100
    pd.options.display.width = 100
    pd.options.mode.chained_assignment = None


def setup_notebook_output():
    """Sets up a notebook's output config.

    Util function which calls all other notebook config setups (except for logging).
    Can be called at the start of a notebook to set various output display configurations to be
    better/more user friendly.
    """
    setup_notebook_pandas_config()
    setup_notebook_matplotlib_config()


def relative_path(path: str, file_path: str) -> pathlib.Path:
    """Form path of the relative path from __file__'s directory."""
    return (pathlib.Path(file_path).parent.absolute() / path).absolute()
