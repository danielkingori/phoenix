# type: ignore
"""Date parsing utilities for Facebook Comment parser.

This script does the following:
1. Recognize and parse any date that has a normal format "June 16 at 4:31 PM"
2. Any date with a weekday or relative designation needs the following:
-- get the retrieved_date from the SingleFile header data (get_retrieved_date())
-- parse date relative to the retrieved_date

Warning: Time is fickle and this script is as perfect as we can make it at this time.
         There may be errors, but we've done our best ;-)
"""
import datetime
import re

from bs4 import Comment as bComment
from dateutil.parser import parse


# TODO: bring into line with mypy

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
TIMECODE = "%Y-%m-%d %H:%M:%S"


def get_retrieved_date(soup):
    """Finds and parses SingleFile comment at top of html doc."""
    m = soup.find(text=lambda text: isinstance(text, bComment))
    n = re.search(r"saved date:(.+?)\n", m)
    clean_date = n.group(0)
    retrieved_at_dt = parse(clean_date.strip(), fuzzy=True)
    return retrieved_at_dt


def utc_adjust_datestring(created_at_dt):
    """Get string from datetime object."""
    if created_at_dt.time() != datetime.time(0, 0):
        utc = created_at_dt.replace(tzinfo=None) + created_at_dt.utcoffset()
    else:
        utc = created_at_dt.replace(tzinfo=None)
    return utc


def return_datestring(date):
    """Return a date string in TIMECODE format."""
    return date.strftime(TIMECODE)


def evaluate_hours(retrieved, date):
    """Evaluate date strings like '5 hours ago' or '14 hrs'."""
    return retrieved - datetime.timedelta(hours=int(date[0]))


def evaluate_minutes(retrieved, date):
    """Evaluate date strings like '14 minutes ago'."""
    return retrieved - datetime.timedelta(minutes=int(date[0]))


def evaluate_weekdays(retrieved, date, created):
    """Evaluate date strings like 'Tuesday at 3:25 PM'."""
    raw_wkday = WEEKDAYS.index(date[0])  # gives the created_at day
    day_diff = retrieved.weekday() - raw_wkday
    if retrieved.weekday() > raw_wkday:
        day_diff = retrieved.weekday() - raw_wkday
    elif retrieved.weekday() == raw_wkday:
        day_diff = 7
    elif retrieved.weekday() < raw_wkday:
        day_diff = 7 - (raw_wkday - retrieved.weekday())

    new_date = retrieved - datetime.timedelta(days=day_diff)
    new_date = new_date.replace(hour=created.hour, minute=created.minute)
    return new_date


def evaluate_yesterday(retrieved, created):
    """Evaluate date strings like 'Yesterday at 12:50 PM'."""
    new_date = retrieved - datetime.timedelta(days=1)
    new_date = new_date.replace(hour=created.hour, minute=created.minute)
    return new_date


def evaluate_today(retrieved, created):
    """Evaluate date strings like 'Today at 3:18 AM'."""
    new_date = retrieved
    new_date = new_date.replace(hour=created.hour, minute=created.minute)
    return new_date


def evaluate_date(retrieved, created):
    """Evaluate date strings like 'May 21 at 10:28 AM'."""
    new_date = created.replace(tzinfo=retrieved.tzinfo)
    if created.year > retrieved.year:
        # This is a special case if a page is evaluated a year after it's retrieved and
        #  it has no year information, it will replace current year with retrieved year
        #  with the exception being something created in a year earlier than the retrieved
        #  date
        new_date = new_date.replace(year=retrieved.year)
    return new_date


def convert_date(date_string, created, retrieved):
    """Converts date from descriptive days to informational."""
    raw_list = date_string.split()
    if (
        "hours ago" in date_string
        or "hr" in date_string
        or "hrs" in date_string
        or "hour" in date_string
    ):
        # offset hours from retrieved
        new_date = evaluate_hours(retrieved, raw_list)
    elif "minutes ago" in date_string:
        # offset minutes from retrieved
        new_date = evaluate_minutes(retrieved, raw_list)
    elif raw_list[0] in WEEKDAYS:
        # offset days from retrieved
        new_date = evaluate_weekdays(retrieved, raw_list, created)
    elif raw_list[0] == "Yesterday":
        # offset by 1 for yesterday
        new_date = evaluate_yesterday(retrieved, created)
    elif raw_list[0] == "Today":  # Eg. 'Today at 2:35 PM'
        # Use retrieved at date, and replace hours and minutes
        new_date = evaluate_today(retrieved, created)
    else:
        # If date_string is just a date, it will pass through here. It is
        # naive so it needs the timezone info added
        new_date = evaluate_date(retrieved, created)
    return new_date


def check_if_just_now(date_raw, retrieved_dt):
    """Check if the date string is 'Just now', which crashes the parser."""
    created_at_dt = None
    if date_raw.strip() == "Just now":
        created_at_dt = retrieved_dt
    return created_at_dt


def parse_date_raw(date_raw):
    """Parse date string into general datetime object."""
    return parse(date_raw.strip(), fuzzy=True)


def main(soup, date_raw):  # noqa
    """Runs the date_parser process for parsing fb pages."""
    # 1. find and parse date of retrieval from SingleFile
    retrieved_at_dt = get_retrieved_date(soup)
    # check if the text is 'Just now' because then parse throws an error
    #  and it's within 1 minute
    created_at_dt = check_if_just_now(date_raw, retrieved_at_dt)
    if not created_at_dt:
        created_at_dt = parse_date_raw(date_raw)
    # Now adjust for days of week / hours / minutes
    created_at_dt = convert_date(date_raw, created_at_dt, retrieved_at_dt)
    # Adjust to utc
    created_utc_dt = utc_adjust_datestring(created_at_dt)
    created_str = return_datestring(created_utc_dt)
    return created_str
