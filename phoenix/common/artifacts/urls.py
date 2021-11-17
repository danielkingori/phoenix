"""Utilities for creating URLs for artifacts.

Includes the local artifacts function.
Be aware that the functions in this script depend on the
placement of the file. As such if the file is moved please update.
"""

import pathlib


ARTIFACTS_PATH = pathlib.Path(__file__).parents[3] / "local_artifacts"

STATIC_DATA_PATH = pathlib.Path(__file__).parents[1] / "static_data"


def get_local() -> str:
    """Get the URL of the local_artifacts."""
    return "file://" + str(ARTIFACTS_PATH) + "/"


def get_static_data() -> str:
    """Get the URL of the static data files directory."""
    return "file://" + str(STATIC_DATA_PATH) + "/"
