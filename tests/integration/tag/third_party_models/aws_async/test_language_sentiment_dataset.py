"""Integration language dataset functionality."""
import datetime

import pandas as pd
import pytest

from phoenix.common import run_datetime
from phoenix.tag.third_party_models.aws_async import language_sentiment_dataset


@pytest.fixture
def language_sentiment_dataset_url(tmpdir_url):
    """Language sentiment dataset url."""
    return f"{tmpdir_url}/language_sentiment_dataset/"


def test_persist_get(language_sentiment_dataset_url):
    """Test persist of language_sentiment_dataset."""
    df = pd.DataFrame(
        {
            "object_id": ["1", "2"],
            "object_url": ["url_1", "url_2"],
            "language_sentiment": ["POSITIVE", "NEGATIVE"],
        }
    )
    run_dt = run_datetime.RunDatetime(
        dt=datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc)
    )

    art_df = language_sentiment_dataset.persist(language_sentiment_dataset_url, df, run_dt)
    read_df = language_sentiment_dataset.get(language_sentiment_dataset_url)

    assert art_df.url == f"{language_sentiment_dataset_url}{run_dt.to_file_safe_str()}.parquet"
    pd.testing.assert_frame_equal(art_df.dataframe, df)
    pd.testing.assert_frame_equal(read_df, df)

    df_2 = pd.DataFrame(
        {
            "object_id": ["3", "4"],
            "object_url": ["url_3", "url_4"],
            "language_sentiment": ["POSITIVE", "NEGATIVE"],
        }
    )

    run_dt = run_datetime.RunDatetime(
        dt=datetime.datetime(2000, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    )

    art_df = language_sentiment_dataset.persist(language_sentiment_dataset_url, df_2, run_dt)
    read_df_2 = language_sentiment_dataset.get(language_sentiment_dataset_url)
    assert art_df.url == f"{language_sentiment_dataset_url}{run_dt.to_file_safe_str()}.parquet"
    pd.testing.assert_frame_equal(art_df.dataframe, df_2)

    expected_read_df_2 = pd.concat([df, df_2])
    pd.testing.assert_frame_equal(read_df_2, expected_read_df_2)
