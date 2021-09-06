"""Group source by functionality."""
from typing import Iterator

import dataclasses
import datetime


@dataclasses.dataclass
class GroupFilters:
    """Group Filters that will be used to group the objects."""

    YEAR_FILTER: int
    MONTH_FILTER: int


def get_group_filters(
    objects_scraped_since: datetime.datetime, objects_scraped_till: datetime.datetime
) -> Iterator[GroupFilters]:
    """Get months and years between the datetimes."""
    if objects_scraped_since > objects_scraped_till:
        raise ValueError(
            f"Start date {objects_scraped_since} is not before end date {objects_scraped_till}"
        )

    year = objects_scraped_since.year
    month = objects_scraped_since.month

    while (year, month) <= (objects_scraped_till.year, objects_scraped_till.month):
        yield GroupFilters(year, month)

        # Move to the next month.  If we're at the end of the year, wrap around
        # to the start of the next.
        #
        # Example: Nov 2017
        #       -> Dec 2017 (month += 1)
        #       -> Jan 2018 (end of year, month = 1, year += 1)
        #
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    return []
