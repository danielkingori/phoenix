"""Utils tests."""
import datetime

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
