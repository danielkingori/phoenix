# type: ignore
"""Test the FB Comment Parser date parser."""
import datetime

import mock
from bs4 import BeautifulSoup
from dateutil import tz
from dateutil.parser import parse

from phoenix.scrape.fb_comment_parser import date_parser


TEST_RETRIEVED_DATE_FILE = "tests/unit/scrape/test_date-parser.html"
TZ_OFFSET = tz.tzoffset(None, -10800)
TEST_RETRIEVED_DATE = datetime.datetime(2020, 7, 21, 17, 0, tzinfo=TZ_OFFSET)


def load_test_file(file=TEST_RETRIEVED_DATE_FILE):
    """Load test html file."""
    with open(file, "r", encoding="utf-8") as f:
        file = f.read()
    return file


def test_get_retrieved_date():
    """Test get_retrieved_date."""
    # Expected behavior
    offset = tz.tzoffset(None, -10800)
    test_timestamp = datetime.datetime(2020, 7, 21, 17, 0, tzinfo=offset)
    # Load parameters
    file = load_test_file()
    soup = BeautifulSoup(file, "html.parser")
    # Execute function
    retrieved = date_parser.get_retrieved_date(soup)
    # Assert behavior
    assert test_timestamp == retrieved


def test_return_datestring():
    """Test return_datestring."""
    # Expected behavior
    expected_date = "2020-07-21 14:00:00"
    # Load parameters
    test_date = datetime.datetime(2020, 7, 21, 14, 0, 0, tzinfo=None)
    # Execute function
    returned_date = date_parser.return_datestring(test_date)
    # Assert behavior
    assert returned_date == expected_date


def test_utc_adjust_datestring():
    """Test utc_adjust_datestring."""
    # Expected behavior
    expected_dates = [
        datetime.datetime(2020, 7, 21, 14, 0, 0, tzinfo=None),
        datetime.datetime(2014, 12, 26, 0, 0, tzinfo=None),
    ]
    # Load parameters
    test_timestamps = [
        datetime.datetime(2020, 7, 21, 17, 0, tzinfo=TZ_OFFSET),
        datetime.datetime(2014, 12, 26, 0, 0, tzinfo=TZ_OFFSET),
    ]
    # Execute function
    returned_dates = []
    for date in test_timestamps:
        returned_dates.append(date_parser.utc_adjust_datestring(date))
    # Assert behavior
    assert returned_dates == expected_dates


def test_evaluate_hours():
    """Test evaluate_hours."""
    # Expected behavior
    expected_date = datetime.datetime(2020, 7, 21, 14, 0, tzinfo=TZ_OFFSET)
    # Load parameters
    retrieved_time = TEST_RETRIEVED_DATE
    date_string = "3 hours ago"
    returned_date = date_parser.evaluate_hours(retrieved_time, date_string.split())
    # Assert behavior
    assert returned_date == expected_date


def test_evaluate_minutes():
    """Test evaluate_minutes."""
    # Expected behavior
    expected_date = datetime.datetime(2020, 7, 21, 16, 46, tzinfo=TZ_OFFSET)
    # Load parameters
    retrieved_time = TEST_RETRIEVED_DATE
    date_string = "14 minutes ago"
    returned_date = date_parser.evaluate_minutes(retrieved_time, date_string.split())
    # Assert behavior
    assert returned_date == expected_date


def test_evaluate_weekdays():
    """Test evaluate_weekdays."""
    # Expected behavior
    expected_dates = [
        datetime.datetime(2020, 7, 14, 15, 25, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 20, 15, 25, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 15, 15, 25, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 17, 15, 25, tzinfo=TZ_OFFSET),
    ]
    # Load parameters
    retrieved_time = TEST_RETRIEVED_DATE
    date_strings = [
        "Tuesday at 3:25 PM",
        "Monday at 3:25 PM",
        "Wednesday at 3:25 PM",
        "Friday at 3:25 PM",
    ]
    # Execute function
    returned_dates = []
    for date_string in date_strings:
        false_parse = parse(date_string, fuzzy=True)
        returned_dates.append(
            date_parser.evaluate_weekdays(retrieved_time, date_string.split(), false_parse)
        )
    # Assert behavior
    assert returned_dates == expected_dates


def test_evaluate_yesterday():
    """Test evaluate_yesterday."""
    # Expected behavior
    expected_date = datetime.datetime(2020, 7, 20, 12, 50, tzinfo=TZ_OFFSET)
    # Load parameters
    retrieved_time = TEST_RETRIEVED_DATE
    date_string = "Yesterday at 12:50 PM"
    false_parse = parse(date_string, fuzzy=True)
    # Execute function
    returned_date = date_parser.evaluate_yesterday(retrieved_time, false_parse)
    # Assert behavior
    assert returned_date == expected_date


def test_evaluate_today():
    """Test evaluate_today."""
    # Expected behavior
    expected_date = datetime.datetime(2020, 7, 21, 3, 18, tzinfo=TZ_OFFSET)
    # Load parameters
    retrieved_time = TEST_RETRIEVED_DATE
    date_string = "Today at 3:18 AM"
    false_parse = parse(date_string, fuzzy=True)
    # Execute function
    returned_date = date_parser.evaluate_today(retrieved_time, false_parse)
    # Assert behavior
    assert returned_date == expected_date


def test_convert_date():
    """Test convert_date."""
    # Expected behavior
    expected_dates = [
        datetime.datetime(2020, 7, 21, 17, 0, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 21, 16, 46, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 21, 12, 0, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 21, 3, 0, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 21, 3, 18, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 20, 12, 50, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 7, 14, 15, 25, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 5, 21, 10, 28, tzinfo=TZ_OFFSET),
        datetime.datetime(2020, 5, 27, 0, 0, tzinfo=TZ_OFFSET),
        datetime.datetime(2019, 12, 3, 7, 18, tzinfo=TZ_OFFSET),
    ]
    # Load parameters
    retrieved_time = TEST_RETRIEVED_DATE
    date_strings = [
        "Just now",
        "14 minutes ago",
        "5 hours ago",
        "14 hrs",
        "Today at 3:18 AM",
        "Yesterday at 12:50 PM",
        "Tuesday at 3:25 PM",
        "May 21 at 10:28 AM",
        "May 27",
        "December 3, 2019 at 7:18 AM",
    ]
    returned_dates = []
    # Execute function
    for date_string in date_strings:
        if date_string == "Just now":
            false_parse = retrieved_time
        else:
            false_parse = parse(date_string, fuzzy=True)
        returned_dates.append(date_parser.convert_date(date_string, false_parse, retrieved_time))
    # Assert behavior
    assert returned_dates == expected_dates


@mock.patch("phoenix.scrape.fb_comment_parser.date_parser.get_retrieved_date")
def test_main(m_get_retrieved_date):
    # Expected behavior
    expected_dates = [
        "2014-12-26 00:00:00",
        "2020-07-21 14:00:00",
        "2020-07-21 00:18:00",
    ]
    # Load parameters
    dates_raw = [
        "Dec 26, 2014",
        "Just now",
        "Today at 3:18 AM",
    ]
    soup = mock.Mock()
    m_get_retrieved_date.return_value = TEST_RETRIEVED_DATE
    # Execute function
    returned_dates = []
    for date in dates_raw:
        returned_dates.append(date_parser.main(soup, date))
    # Assert behavior
    assert returned_dates == expected_dates
