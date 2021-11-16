"""RunDatetime tests."""
import datetime

import pytest
import pytz
from freezegun import freeze_time

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
    timezone = pytz.timezone("America/Los_Angeles")
    dt = timezone.localize(datetime.datetime.now())
    with pytest.raises(ValueError):
        run_datetime.RunDatetime(dt)


def test_disallowed_datetime_timezone_st():
    """Test disallowed datetimes."""
    tz = datetime.timezone(datetime.timedelta(seconds=19800))
    dt = datetime.datetime.now().astimezone(tz)
    with pytest.raises(ValueError):
        run_datetime.RunDatetime(dt)


@pytest.mark.parametrize(
    "dt,expected_str,date_str",
    [
        (
            datetime.datetime(2000, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc),
            "20000102T030405.000006Z",
            "2000-01-02",
        ),
        # To show that micro seconds can be processed and are expected
        # however Run datetimes should be
        (
            datetime.datetime(2000, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            "20000102T030405.000000Z",
            "2000-01-02",
        ),
        (
            datetime.datetime(2010, 11, 12, 13, 14, 15, 16, tzinfo=datetime.timezone.utc),
            "20101112T131415.000016Z",
            "2010-11-12",
        ),
    ],
)
def test_to_file_safe_str(dt, expected_str, date_str):
    """Test of the to_file_name_meta."""
    run_dt = run_datetime.RunDatetime(dt)
    assert run_dt.to_file_safe_str() == expected_str
    assert run_dt.to_run_date_str() == date_str


def test_create_run_datetime_now():
    """Test create_run_datetime_now returns correct time."""
    dt = datetime.datetime(2000, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    with freeze_time(dt):
        result = run_datetime.create_run_datetime_now()
        assert dt == result.dt


@pytest.mark.parametrize(
    "expected_timestamp, run_datetime_str",
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
def test_from_file_safe_str(expected_timestamp, run_datetime_str):
    """Test from_file_safe_str."""
    result = run_datetime.from_file_safe_str(run_datetime_str)
    assert expected_timestamp == result.dt


def test_eq():
    """Test equality."""
    run_dt_1 = run_datetime.RunDatetime(
        datetime.datetime(2010, 11, 12, 13, 14, 15, 16, tzinfo=datetime.timezone.utc)
    )
    run_dt_2 = run_datetime.RunDatetime(
        datetime.datetime(2010, 11, 12, 13, 14, 15, 16, tzinfo=datetime.timezone.utc)
    )
    assert run_dt_1 == run_dt_2


def test_eq_non():
    """Test equality."""
    run_dt_1 = run_datetime.RunDatetime(
        datetime.datetime(2010, 11, 12, 13, 14, 15, 17, tzinfo=datetime.timezone.utc)
    )
    run_dt_2 = run_datetime.RunDatetime(
        datetime.datetime(2010, 11, 12, 13, 14, 15, 16, tzinfo=datetime.timezone.utc)
    )
    assert run_dt_1 != run_dt_2


def test_url_config():
    """Test to_url_config."""
    run_dt = run_datetime.RunDatetime(
        datetime.datetime(2010, 11, 12, 13, 14, 15, 17, tzinfo=datetime.timezone.utc)
    )
    assert run_dt.to_url_config() == {"YEAR_FILTER": 2010, "MONTH_FILTER": 11}
