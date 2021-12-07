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
                timestamp_prefix=None,
                timestamp_suffix=None,
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
                timestamp_prefix=None,
                timestamp_suffix=None,
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
                timestamp_prefix=None,
                timestamp_suffix=None,
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
                timestamp_prefix=None,
                timestamp_suffix=None,
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
                timestamp_prefix="posts-",
                timestamp_suffix=None,
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
                timestamp_prefix="usr_tweets-",
                timestamp_suffix=None,
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
                timestamp_prefix="keyword_tweets-",
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/posts-20210612T200950.425433Z.json",
            source_file_name_processing.SourceFileName(
                is_legacy=False,
                full_url="file:///host/posts-20210612T200950.425433Z.json",
                folder_url="file:///host",
                timestamp_prefix="posts-",
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/usr_tweets-20210612T200950.425433Z.json",
            source_file_name_processing.SourceFileName(
                is_legacy=False,
                full_url="file:///host/usr_tweets-20210612T200950.425433Z.json",
                folder_url="file:///host",
                timestamp_prefix="usr_tweets-",
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
        ),
        (
            "file:///host/usr_tweets-20210612T200950.425433Z-suf.json",
            source_file_name_processing.SourceFileName(
                is_legacy=False,
                full_url="file:///host/usr_tweets-20210612T200950.425433Z-suf.json",
                folder_url="file:///host",
                timestamp_prefix="usr_tweets-",
                timestamp_suffix="-suf",
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


@pytest.mark.parametrize(
    "source_file_name, non_legacy_file_name",
    [
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/dir/2021-05-25T17\uf03a39\uf03a04.914162.json",
                folder_url="file:///host/dir",
                timestamp_prefix=None,
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
            "20210525T173904.914162Z.json",
        ),
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/2021-05-25T17:39:04.914162.json",
                folder_url="file:///host",
                timestamp_prefix=None,
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
            "20210525T173904.914162Z.json",
        ),
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/prefix-2021-05-25T17:39:04.914162+04:00-suf.json",
                folder_url="file:///host",
                timestamp_prefix="prefix-",
                timestamp_suffix="-suf",
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
            "prefix-20210525T173904.914162Z-suf.json",
        ),
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/prefix-2021-05-25T17:39:04.914162+04:00.json",
                folder_url="file:///host",
                timestamp_prefix="prefix-",
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 13, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
            "prefix-20210525T133904.914162Z.json",
        ),
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/2021-05-25T17:39:04.914162+04:00-suf.json",
                folder_url="file:///host",
                timestamp_prefix=None,
                timestamp_suffix="-suf",
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 13, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
            "20210525T133904.914162Z-suf.json",
        ),
    ],
)
def test_non_legacy_file_name(source_file_name, non_legacy_file_name):
    """None legacy file name test."""
    assert source_file_name.non_legacy_file_name() == non_legacy_file_name


@pytest.mark.parametrize(
    "source_file_name, expected_basename",
    [
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/2021-05-25T17:39:04.914162.json",
                folder_url="file:///host",
                timestamp_prefix=None,
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 5, 25, 17, 39, 4, 914162, tzinfo=datetime.timezone.utc)
                ),
            ),
            "2021-05-25T17:39:04.914162.json"
        ),
        (
            source_file_name_processing.SourceFileName(
                is_legacy=True,
                full_url="file:///host/dir/usr_tweets-2021-06-12T20_09_50.425433.json",
                folder_url="file:///host/dir",
                timestamp_prefix="usr_tweets-",
                timestamp_suffix=None,
                extension=".json",
                run_dt=run_datetime.RunDatetime(
                    datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc)
                ),
            ),
            "usr_tweets-2021-06-12T20_09_50.425433.json",
        ),
    ],
)
def test_get_basename(source_file_name, expected_basename):
    """Test get_basename."""
    assert source_file_name.get_basename() == expected_basename
