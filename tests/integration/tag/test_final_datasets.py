"""Test the final datasets."""
import datetime
import glob

import pandas as pd
import pytest

from phoenix.common import artifacts
from phoenix.tag import final_datasets


def test_persist_overwriting_partitions(tmp_path):
    """Test for the overwrite of "dynamic" partitions."""
    test_artefact_dir = tmp_path / "dataframe_artifacts" / "df"
    artefact_url = f"{test_artefact_dir}"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "year_filter": [2021, 2021, 2022, 2022, 2022],
            "month_filter": [12, 12, 1, 1, 2],
            "day_filter": [1, 2, 1, 1, 3],
            "date_filter": [
                datetime.datetime(2021, 12, 1),
                datetime.datetime(2021, 12, 2),
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2022, 2, 3),
            ],
        }
    )
    e_df = df.copy()

    a_df = final_datasets.persist(artefact_url, df)
    e_url = artefact_url
    assert a_df.url == e_url
    # The original df is returned
    pd.testing.assert_frame_equal(a_df.dataframe.compute(), e_df)

    a_df_2 = final_datasets.read(artefact_url)
    assert a_df.url == a_df_2.url

    e_df["year_filter"] = e_df["year_filter"].astype("category")
    e_df["month_filter"] = e_df["month_filter"].astype("category")
    e_df["day_filter"] = e_df["day_filter"].astype("category")
    e_df = e_df.set_index(["year_filter", "month_filter", "day_filter"]).sort_index()
    pd.testing.assert_frame_equal(e_df, a_df_2.dataframe.compute())

    parquet_files = glob.glob(a_df.url + "/**/*.parquet", recursive=True)
    expected_subdir = [
        "/year_filter=2021/month_filter=12/day_filter=1/part.0.parquet",
        "/year_filter=2021/month_filter=12/day_filter=2/part.0.parquet",
        "/year_filter=2022/month_filter=1/day_filter=1/part.0.parquet",
        "/year_filter=2022/month_filter=2/day_filter=3/part.0.parquet",
    ]
    for sub_dir in expected_subdir:
        matched_subdir = [f for f in parquet_files if sub_dir in f]
        assert len(matched_subdir) == 1

    df_2 = pd.DataFrame(
        {
            "id": [4, 101, 102, 103, 104],
            "year_filter": [2022, 2022, 2022, 2022, 2022],
            "month_filter": [1, 1, 2, 2, 3],
            "day_filter": [1, 2, 1, 3, 3],
            "date_filter": [
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2022, 1, 2),
                datetime.datetime(2022, 2, 1),
                datetime.datetime(2022, 2, 3),
                datetime.datetime(2022, 3, 3),
            ],
        }
    )
    e_df_2 = df_2.copy()

    # It is important to partition correctly using 1 partition is a simple way to do this
    a_df_3 = final_datasets.persist(artefact_url, df_2)
    assert a_df_3.url == e_url
    pd.testing.assert_frame_equal(a_df_3.dataframe.compute(), e_df_2)

    a_df_4 = final_datasets.read(artefact_url)
    assert a_df_4.url == e_url
    df_filtered = df[df["year_filter"] == 2021]
    df_filtered = df_filtered[df_filtered["id"] != 4]
    e_df_2 = pd.concat([df_filtered, df_2.copy()])
    e_df_2["year_filter"] = e_df_2["year_filter"].astype("category")
    e_df_2["month_filter"] = e_df_2["month_filter"].astype("category")
    e_df_2["day_filter"] = e_df_2["day_filter"].astype("category")
    e_df_2 = e_df_2.set_index(["year_filter", "month_filter", "day_filter"]).sort_index()
    # The expected df for the get has typed the partition columns as categories
    pd.testing.assert_frame_equal(e_df_2, a_df_4.dataframe.compute())

    # Check the correct parquet files have been created
    parquet_files = glob.glob(a_df.url + "/**/*.parquet", recursive=True)
    assert len(parquet_files) == 7
    expected_subdir = [
        "/year_filter=2021/month_filter=12/day_filter=1/part.0.parquet",
        "/year_filter=2021/month_filter=12/day_filter=2/part.0.parquet",
        "/year_filter=2022/month_filter=1/day_filter=1/part.0.parquet",
        "/year_filter=2022/month_filter=1/day_filter=2/part.0.parquet",
        "/year_filter=2022/month_filter=2/day_filter=1/part.0.parquet",
        "/year_filter=2022/month_filter=2/day_filter=3/part.0.parquet",
        "/year_filter=2022/month_filter=3/day_filter=3/part.0.parquet",
    ]
    for sub_dir in expected_subdir:
        matched_subdir = [f for f in parquet_files if sub_dir in f]
        assert len(matched_subdir) == 1

    artifacts.dask_dataframes.delete(a_df_4)

    with pytest.raises(FileNotFoundError):
        final_datasets.read(a_df_2.url)
