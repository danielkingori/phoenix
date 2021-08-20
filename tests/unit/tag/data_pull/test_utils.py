"""Data Pull utils."""
import datetime

import pytest
from freezegun import freeze_time

from phoenix.tag.data_pull import utils


@freeze_time("2021-01-14 03:21:34", tz_offset=0)
@pytest.mark.parametrize(
    "url, expected_timestamp",
    [
        ("", datetime.datetime(2021, 1, 14, 3, 21, 34, tzinfo=datetime.timezone.utc)),
        (
            "file:///host/2021-05-25T173904.914162.json",
            datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/2021-05-25T17:39:04.914162.json",
            datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/2021-05-25T17:39:04.914162+04:00.json",
            datetime.datetime(2021, 5, 25, 13, 39, 4, 914162, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/2021-05-25T17:39:04.914162+00:00.json",
            datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/posts-2021-06-12T20_09_50.425433.json",
            datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/usr_tweets-2021-06-12T20_09_50.425433.json",
            datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/keyword_tweets-2021-06-12T20_09_50.425433.json",
            datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_get_file_name_timestamp(url, expected_timestamp):
    """Test get_file_name_timestamp."""
    result = utils.get_file_name_timestamp(url)
    assert expected_timestamp == result
