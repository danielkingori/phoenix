"""Test Group By functionality."""
import datetime

import pytest

from phoenix.scrape import group_by


@pytest.mark.parametrize(
    "objects_scraped_since, objects_scraped_till, expected_list",
    [
        (
            datetime.datetime(2001, 12, 31),
            datetime.datetime(2002, 2, 1),
            [
                group_by.GroupFilters(2001, 12),
                group_by.GroupFilters(2002, 1),
                group_by.GroupFilters(2002, 2),
            ],
        ),
        (
            datetime.datetime(2001, 12, 1),
            datetime.datetime(2001, 12, 2),
            [
                group_by.GroupFilters(2001, 12),
            ],
        ),
    ],
)
def test_get_group_filters(objects_scraped_since, objects_scraped_till, expected_list):
    """Test get_group_filters."""
    assert expected_list == list(
        group_by.get_group_filters(objects_scraped_since, objects_scraped_till)
    )
