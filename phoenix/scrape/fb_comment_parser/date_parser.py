# type: ignore
"""Date parsing utilities for Facebook Comment parser."""
import re
from datetime import timedelta

from bs4 import Comment as bComment
from dateutil.parser import parse


# TODO: bring into line with mypy
# This script is not fully functional yet.
# This script does the following:
# 1. Recognize and parse any date that has a normal format "June 16 at 4:31 PM"
# 2. Any date with a weekday or relative designation needs the following:
# -- get the retrieved_date from the SingleFile header data (get_retrieved_date())
# -- parse date relative to the retrieved_date
# Known bugs: When a date with no time comes in, it gets shifted +/-
#             by the UTC conversion and can often end up on the previous day.


def return_datestring(dt_obj):  # noqa
    """Get string from datetime object.

    Args:
      dt_obj: datetime object
    Returns:
      string: UTC adjusted 'year-month-day hour:minute:second'
    following this advice to normalize to utc
    """
    utc = dt_obj.replace(tzinfo=None) + dt_obj.utcoffset()
    return utc.strftime("%Y-%m-%d %H:%M:%S")


def convert_date(date_string, created, retrieved):  # noqa
    """Converts date from descriptive days to informational.

    Args:
      date_string: string with original date eg. 'Sunday at 12:05 AM'
      created: parsed datetime object of the date_string
      retrieved: parsed retrieval date from SingleFile comment
    Returns:
      new_date: timezone aware datetime object
    """
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    raw_list = date_string.split()

    if "hours ago" in date_string or "hr" in date_string or "hrs" in date_string:
        # offset hours from retrieved
        new_date = retrieved - timedelta(hours=int(date_string.split()[0]))
    elif "minutes ago" in date_string:
        # offset minutes from retrieved
        new_date = retrieved - timedelta(minutes=int(date_string.split()[0]))
    elif raw_list[0] in weekdays:
        # offset days from retrieved
        raw_wkday = weekdays.index(raw_list[0])  # gives the created_at day
        day_diff = retrieved.weekday() - raw_wkday
        if day_diff < 0:
            day_diff = day_diff + 7  # this offsets for rollover days like Monday-Sunday
        new_date = retrieved - timedelta(days=day_diff)
        new_date = new_date.replace(hour=created.hour, minute=created.minute)
    elif raw_list[0] == "Yesterday":
        # offset by 1 for yesterday
        new_date = retrieved - timedelta(days=1)
        new_date = new_date.replace(hour=created.hour, minute=created.minute)
    elif raw_list[0] == "Today":  # Eg. 'Today at 2:35 PM'
        # Use retrieved at date, and replace hours and minutes
        new_date = retrieved
        new_date = new_date.replace(hour=created.hour, minute=created.minute)
    else:
        # If date_string is just a date, it will pass through here. It is
        # naive so it needs the timezone info added
        new_date = created.replace(tzinfo=retrieved.tzinfo)
    return new_date


def get_retrieved_date(soup):  # noqa
    """finds and parses the date of retrieval of the file from
        the saved SingleFile comment at top of html doc.

    Args:
      soup: BeautifulSoup object of the retrieved html page
    Returns:
      retrieved_at_dt: timezone aware datetime object
    """
    m = soup.find(text=lambda text: isinstance(text, bComment))
    n = re.search(r"saved date:(.+?)\n", m)
    retrieved_at_dt = parse(n.group(0).strip(), fuzzy=True)
    return retrieved_at_dt


def main(soup, date_raw):  # noqa
    """runs the date_parser process for parsing fb pages.

    Args:
      soup: BeautifulSoup object of the retrieved html page
      date_raw: raw date as seen on facebook eg. 'Sunday at 12:05 AM'
    Returns:
      retrieved_at_dt: timezone aware datetime object
    """
    # 1. find and parse date of retrieval from SingleFile
    retrieved_at_dt = get_retrieved_date(soup)

    # 2. Parse the date - if it's just a date, this is over
    #    If it has days of week written, it will be adjusted to current date.
    try:
        created_at_dt = parse(date_raw.strip(), fuzzy=True)

    # 3. Except date can't be parsed, so check if
    #   date created 'Just now' can use retrieved_at_dt as date
    except ValueError:
        if date_raw.strip() == "Just now":
            created_at_dt = retrieved_at_dt

    # Now adjust for days of week and convert to string adjusted to UTC
    created_at_dt = convert_date(date_raw, created_at_dt, retrieved_at_dt)
    created_str = return_datestring(created_at_dt)

    return created_str
