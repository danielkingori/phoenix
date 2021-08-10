# type: ignore
"""Integration test the FB Comment Parser date parser."""
import datetime

from bs4 import BeautifulSoup
from dateutil import tz

from phoenix.scrape.fb_comment_parser import date_parser


TEST_RETRIEVED_DATE_FILE = "tests/integration/scrape/test_date-parser.html"


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
