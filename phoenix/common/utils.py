"""General utilities."""
from typing import Literal, Optional

import datetime
import logging
import os
import pathlib
import sys

import matplotlib.pyplot as plt
import pandas as pd
from dask.distributed import Client


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


def setup_notebook_pandas_config(
    max_rows: int = 100,
    max_columns: int = 100,
    width: int = 100,
    chained_assignment: Optional[Literal["warn", "raise"]] = None,
):
    """Sets a good pandas display config, primarily not truncating rows in dataframe outputs.

    Args:
        max_rows (int): maximum number of rows to display
        max_columns (int): maximum number of columns to display
        width (int): display width in number of characters
        chained_assignment (Optional[Literal["warn", "raise"]]): Trust level
            in user's volition to do an assignment to a chained indexing expression. Sets
            pandas' response to that assignment to be an ignore (None), "warn" the user,
            or "raise" an error.

    Affects calls like:
    >>> from IPython.display import display
    >>> display(df)
    Can be called at the start of a notebook to apply config to rest of notebook.
    """
    pd.options.display.max_rows = max_rows
    pd.options.display.max_columns = max_columns
    pd.options.display.width = width
    pd.options.mode.chained_assignment = chained_assignment


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


def dask_global_init():
    """Initialise the global dask if DASK_CLUSTER_IP is set.

    You can set DASK_CLUSTER_URL for development by doing
    ```
    python
    from dask.distributed import LocalCluster
    cluster = LocalCluster(scheduler_port=8786)
    cluster.scheduler.address
    ```
    This will print out the URL of the cluster
    something like: `tcp://127.0.0.1:8786`.
    `export DASK_CLUSTER_IP=<ip:port>`
    or in the notebook (don't use quotes if in notebook):
    `%env DASK_CLUSTER_IP=<ip:port>`
    You can see the cluster by going to:
    http://localhost:8787/status

    Be aware this doesn't work for docker yet.
    """
    dask_cluster_ip = os.getenv("DASK_CLUSTER_IP")
    if dask_cluster_ip:
        logging.info(f"Dask cluster initialised with URL: {dask_cluster_ip}")
        Client(dask_cluster_ip)
    else:
        logging.info("Dask default is used.")


def is_utc(dt: datetime.datetime):
    """Is the datetime utc."""
    if not dt.tzinfo:
        return False

    utcoffset = dt.utcoffset()
    # Taken from
    # https://stackoverflow.com/questions/6706499/checking-if-date-is-in-utc-format
    if utcoffset and int(utcoffset.total_seconds()) != 0:
        return False
    return True
