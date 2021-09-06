"""Data Pull utils."""
import datetime

import pytest
from freezegun import freeze_time

from phoenix.common import run_datetime
from phoenix.common.artifacts import source_file_name_processing


@freeze_time("2000-01-1 01:01:01", tz_offset=0)
@pytest.mark.parametrize(
    "url, expected_source_file_name",
    [
        ("", None),
        (
            "file:///host/dir/2021-05-25T173904.914162.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/dir/2021-05-25T17\uf03a39\uf03a04.914162.json",
                folder_url="file:///host/dir",
                file_name_prefix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/2021-05-25T17:39:04.914162.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/2021-05-25T17:39:04.914162.json",
                folder_url="file:///host",
                file_name_prefix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/2021-05-25T17:39:04.914162+04:00.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/2021-05-25T17:39:04.914162+04:00.json",
                folder_url="file:///host",
                file_name_prefix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 13, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/2021-05-25T17:39:04.914162+00:00.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/2021-05-25T17:39:04.914162+00:00.json",
                folder_url="file:///host",
                file_name_prefix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/posts-2021-06-12T20_09_50.425433.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/posts-2021-06-12T20_09_50.425433.json",
                folder_url="file:///host",
                file_name_prefix="posts-",
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/usr_tweets-2021-06-12T20_09_50.425433.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/usr_tweets-2021-06-12T20_09_50.425433.json",
                folder_url="file:///host",
                file_name_prefix="usr_tweets-",
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/keyword_tweets-2021-06-12T20_09_50.425433.json",
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/keyword_tweets-2021-06-12T20_09_50.425433.json",
                folder_url="file:///host",
                file_name_prefix="keyword_tweets-",
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
    ],
)
def test_get_file_name_timestamp(url, expected_source_file_name):
    """Test get_file_name_timestamp."""
    result = source_file_name_processing.get_source_file_name(url)
    assert expected_source_file_name == result
