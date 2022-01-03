"""Utils tests."""
import datetime

import pandas as pd
import pytz

from phoenix.common import utils


def test_is_utc_true():
    """Test UTC."""
    dt = datetime.datetime.now(datetime.timezone.utc)
    assert utils.is_utc(dt)


def test_is_utc_no_timezone():
    """Test no timezone."""
    dt = datetime.datetime.now()
    assert not utils.is_utc(dt)


def test_is_utc_none_utc_timezone():
    """Test none UTC timezone."""
    timezone = pytz.timezone("America/Los_Angeles")
    dt = timezone.localize(datetime.datetime.now())
    assert not utils.is_utc(dt)


def test_is_utc_timezone_st():
    """Test seconds timezone ."""
    tz = datetime.timezone(datetime.timedelta(seconds=19800))
    dt = datetime.datetime.now().astimezone(tz)
    assert not utils.is_utc(dt)


def test_setup_notebook_pandas_config_default():
    """Test the default setup of pandas config for notebooks."""
    # test our notebooks' default options are not pandas default options
    assert pd.options.display.max_rows != 100
    assert pd.options.display.max_columns != 100
    assert pd.options.display.width != 100
    assert pd.options.mode.chained_assignment is not None
    utils.setup_notebook_pandas_config()

    # our notebooks' default options
    assert pd.options.display.max_rows == 100
    assert pd.options.display.max_columns == 100
    assert pd.options.display.width == 100
    assert pd.options.mode.chained_assignment is None


def test_setup_notebook_pandas_config_setters():
    """Test the parametrized setup of pandas config for notebooks."""
    # test our parameterized options are not default options
    assert pd.options.display.max_rows != 42
    assert pd.options.display.max_columns != 42
    assert pd.options.display.width != 42
    assert pd.options.mode.chained_assignment != "raise"
    utils.setup_notebook_pandas_config(
        max_rows=42, max_columns=42, width=42, chained_assignment="raise"
    )

    # our notebooks' default options
    assert pd.options.display.max_rows == 42
    assert pd.options.display.max_columns == 42
    assert pd.options.display.width == 42
    assert pd.options.mode.chained_assignment == "raise"
    # reset options back to our options
    utils.setup_notebook_pandas_config()
