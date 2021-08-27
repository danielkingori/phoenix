"""RunDatetime functionality to process run datetimes.

This keeps datetimes consistent across projects.
"""
import datetime


class RunDatetime:
    """RunDatetime is an interface for consistent datetimes."""

    dt: datetime.datetime

    def __init__(self, dt: datetime.datetime):
        """Initialise the RunDatetime.

        It is recommended that a datetime with micro seconds is given.
        datetime objects are build to actively discourage checking
        for micro seconds.
        """
        if not dt.tzinfo:
            raise ValueError("RunDatetime must have time zone UTC")

        utcoffset = dt.utcoffset()

        # Taken from
        # https://stackoverflow.com/questions/6706499/checking-if-date-is-in-utc-format
        if utcoffset and int(utcoffset.total_seconds()) != 0:
            raise ValueError("RunDatetime must have time zone UTC")

        self.dt = dt
