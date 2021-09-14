"""Data Pull utils."""
import datetime

import pytest

from phoenix.tag.data_pull import utils


@pytest.mark.parametrize(
    "url, expected_timestamp",
    [
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
        (
            "file:///host/source-usr_tweets-20210612T200950.425433Z.json",
            datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc),
        ),
        (
            "file:///host/source-keyword_tweets-20210612T200950.425433Z.json",
            datetime.datetime(2021, 6, 12, 20, 9, 50, 425433, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_get_file_name_timestamp(url, expected_timestamp):
    """Test get_file_name_timestamp."""
    result = utils.get_file_name_timestamp(url)
    assert expected_timestamp == result


@pytest.mark.parametrize(
    "url",
    [
        "",
        "file:///host/2021-05-3904.914162.json",
        "file:///host/ffjkldjl-2021-05-3904.914162.json",
        "file:///host/source-keyword_tweets-20210612T200950.425433.json",
    ],
)
def test_get_file_name_timestamp_none(url):
    """Test that with no timestamp then exception."""
    with pytest.raises(RuntimeError) as error:
        utils.get_file_name_timestamp(url)
        assert url in str(error.value)


@pytest.mark.parametrize(
    "url, expected_result",
    [
        ("", False),
        ("file:///host/2021-05-39T010204.914162/", False),
        ("file:///host/source-keyword_tweets-20210612T200950.425433Z.parquet", False),
        ("file:///host/source-keyword_tweets-20210612T200950.425433Z.json", True),
        ("20210612T200950.425433Z.json", True),
    ],
)
def test_valid_file_name(url, expected_result):
    """Valid file name test"""
    assert expected_result == utils.is_valid_file_name(url)
