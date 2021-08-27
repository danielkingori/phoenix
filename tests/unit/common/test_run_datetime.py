"""RunDatetime tests."""
import datetime

import pytest

from phoenix.common import run_datetime


def test_allowed_datetime():
    """Test allowed datetimes."""
    dt = datetime.datetime.now(datetime.timezone.utc)
    run_dt = run_datetime.RunDatetime(dt)
    assert dt == run_dt.dt


def test_disallowed_datetime_no_timezone():
    """Test disallowed datetimes."""
    dt = datetime.datetime.now()
    with pytest.raises(ValueError):
        run_datetime.RunDatetime(dt)


def test_disallowed_datetime_timezone():
    """Test disallowed datetimes."""
    dt = datetime.datetime.now().astimezone()
    with pytest.raises(ValueError):
        run_datetime.RunDatetime(dt)


def test_disallowed_datetime_timezone_st():
    """Test disallowed datetimes."""
    tz = datetime.timezone(datetime.timedelta(seconds=19800))
    dt = datetime.datetime.now().astimezone(tz)
    with pytest.raises(ValueError):
        run_datetime.RunDatetime(dt)
