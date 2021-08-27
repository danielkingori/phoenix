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


@pytest.mark.parametrize(
    "dt,expected_str",
    [
        (
            datetime.datetime(2000, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc),
            "20000102T030405.000006Z",
        ),
        # To show that micro seconds can be processed and are expected
        # however Run datetimes should be
        (
            datetime.datetime(2000, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            "20000102T030405.000000Z",
        ),
        (
            datetime.datetime(2010, 11, 12, 13, 14, 15, 16, tzinfo=datetime.timezone.utc),
            "20101112T131415.000016Z",
        ),
    ],
)
def test_to_file_safe_str(dt, expected_str):
    """Test of the to_file_name_meta."""
    run_dt = run_datetime.RunDatetime(dt)
    assert run_dt.to_file_safe_str() == expected_str
