"""RunDatetime functionality to process run datetimes.

This keeps datetimes consistent across projects.
"""
import datetime


FILE_SAFE_FORMAT = "%Y%m%dT%H%M%S.%fZ"


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

    def to_file_safe_str(self) -> str:
        """Get the run datetime as a string that can be used as a file name.

        This returns a basic iso format that only contains, number and letters.
        https://en.wikipedia.org/wiki/ISO_8601

        This is to make the persisting of files work with any file system; windows, cloud.
        """
        return self.dt.strftime(FILE_SAFE_FORMAT)


def create_run_datetime_now() -> RunDatetime:
    """Create a run datetime for now."""
    return RunDatetime(datetime.datetime.now(datetime.timezone.utc))


def from_file_safe_str(run_datetime_str: str) -> RunDatetime:
    """Create a RunDatetime from a file safe str."""
    dt = datetime.datetime.strptime(run_datetime_str, FILE_SAFE_FORMAT)
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    return RunDatetime(dt)
