"""Artefacts Dask DataFrame functionality test on local file system."""
import glob

import pandas as pd
import pytest
from dask import dataframe as dd

from phoenix.common import artifacts


def test_dataframe_artifact_partitions_hive_folder(tmp_path):
    """Test full functionality of Dask DataFrame artifacts with partitions."""
    test_artefact_dir = tmp_path / "dataframe_artifacts" / "df"
    artefact_url = f"{test_artefact_dir}"
    to_parquet_params = {
        "partition_on": ["year_filter", "month_filter"],
    }

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "year_filter": [2021, 2021, 2022, 2022],
            "month_filter": [12, 12, 1, 2],
        }
    )

    e_df = df.copy()
    e_df["year_filter"] = e_df["year_filter"].astype("category")
    e_df["month_filter"] = e_df["month_filter"].astype("category")
    ddf = artifacts.utils.pandas_to_dask(df)
    e_ddf = artifacts.utils.pandas_to_dask(e_df)

    a_df = artifacts.dask_dataframes.persist(artefact_url, ddf, to_parquet_params)
    e_url = artefact_url
    assert a_df.url == e_url
    # The original df is returned
    pd.testing.assert_frame_equal(a_df.dataframe.compute(), df)

    a_df_2 = artifacts.dask_dataframes.get(artefact_url, to_parquet_params)
    assert a_df.url == a_df_2.url
    # The expected df for the get has typed the partition columns as categories
    pd.testing.assert_frame_equal(e_df, a_df_2.dataframe.compute(), check_index_type=False)
    dd.utils.assert_eq(e_ddf, a_df_2.dataframe)

    # Check the correct parquet files have been created
    parquet_files = glob.glob(a_df.url + "/**/*.parquet", recursive=True)
    # One file for each partition
    assert len(parquet_files) == 3
    expected_subdir = [
        "/year_filter=2021/month_filter=12/",
        "/year_filter=2022/month_filter=1/",
        "/year_filter=2022/month_filter=2/",
    ]
    for sub_dir in expected_subdir:
        matched_subdir = [f for f in parquet_files if sub_dir in f]
        assert len(matched_subdir) == 1

    artifacts.dask_dataframes.delete(a_df_2)

    with pytest.raises(FileNotFoundError):
        artifacts.dask_dataframes.get(a_df_2.url)


def test_dataframe_artifact_partition_overwrite(tmp_path):
    """Test for the overwrite of "dynamic" partitions.

    This test shows that that the overwrite functionality can be
    hacked so that it only overwrites the partitions that are
    being written.

    In pyspark this is called "dynamic" for `partitionOverwriteMode`:
    https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-dynamic-partition-inserts.html

    This is a hack for using dask in this way.
    At some point a ticket should be raised with dask to clarify this.

    The downside of using this technique is the labelling of the divisions
    is lost, hence the use of `check_divisions=False`.
    However this can be fixed by hand when the developer knows the partitioning
    of the dataset. Such as in this case `year_filter` and `month_filter`.

    An other gottcha is the partitioning of the dask dataframe.
    If there are two partitions in a dask dataframe
    it could mean that more then one file is created for a persisted folder partition.
    This will then mean that the overwrite will not happen if the dask dataframe
    is not partitioned in the same way.
    """
    test_artefact_dir = tmp_path / "dataframe_artifacts" / "df"
    artefact_url = f"{test_artefact_dir}"
    to_parquet_params = {
        "engine": "pyarrow",
        "partition_on": ["year_filter", "month_filter"],
        "overwrite": False,
        "write_metadata_file": False,
        "write_index": False,
    }

    read_parquet_params = {"engine": "pyarrow", "index": "id"}

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "year_filter": [2021, 2021, 2022, 2022, 2022],
            "month_filter": [12, 12, 1, 1, 2],
        }
    )
    df = df.set_index("id", drop=False)

    e_df = df.copy()
    e_df["year_filter"] = e_df["year_filter"].astype("category")
    e_df["month_filter"] = e_df["month_filter"].astype("category")
    e_df = e_df.drop("id", axis=1)
    # It is important to partition correctly using 1 partition is a simple way to do this
    ddf = artifacts.utils.pandas_to_dask(df, 1)
    e_ddf = artifacts.utils.pandas_to_dask(e_df)

    a_df = artifacts.dask_dataframes.persist(artefact_url, ddf, to_parquet_params)
    e_url = artefact_url
    assert a_df.url == e_url
    # The original df is returned
    pd.testing.assert_frame_equal(a_df.dataframe.compute(), df)

    a_df_2 = artifacts.dask_dataframes.get(artefact_url, read_parquet_params)
    assert a_df.url == a_df_2.url
    # The expected df for the get has typed the partition columns as categories
    pd.testing.assert_frame_equal(e_df, a_df_2.dataframe.compute(), check_index_type=False)
    dd.utils.assert_eq(e_ddf, a_df_2.dataframe, check_divisions=False)

    # Check the correct parquet files have been created
    parquet_files = glob.glob(a_df.url + "/**/*.parquet", recursive=True)
    expected_subdir = [
        "/year_filter=2021/month_filter=12/",
        "/year_filter=2022/month_filter=1/",
        "/year_filter=2022/month_filter=2/",
    ]
    for sub_dir in expected_subdir:
        matched_subdir = [f for f in parquet_files if sub_dir in f]
        assert len(matched_subdir) == 1

    df_2 = pd.DataFrame(
        {
            "id": [4, 105, 106, 107, 108],
            "year_filter": [2022, 2022, 2022, 2022, 2022],
            "month_filter": [2, 1, 2, 2, 3],
        }
    )
    df_2 = df_2.set_index("id", drop=False)

    df_filtered = df[df["year_filter"] == 2021]
    df_filtered = df_filtered[df_filtered["id"] != 4]
    e_df_2 = pd.concat([df_filtered, df_2.copy()])
    e_df_2["year_filter"] = e_df_2["year_filter"].astype("category")
    e_df_2["month_filter"] = e_df_2["month_filter"].astype("category")
    e_df_2 = e_df_2.drop("id", axis=1)
    e_df_2 = e_df_2.sort_values(by=["year_filter", "month_filter", "id"])
    # It is important to partition correctly using 1 partition is a simple way to do this
    ddf_2 = artifacts.utils.pandas_to_dask(df_2, 1)
    e_ddf = artifacts.utils.pandas_to_dask(e_df_2)

    a_df_3 = artifacts.dask_dataframes.persist(artefact_url, ddf_2, to_parquet_params)
    assert a_df_3.url == e_url
    # The original df is returned
    pd.testing.assert_frame_equal(a_df_3.dataframe.compute(), df_2)

    a_df_4 = artifacts.dask_dataframes.get(artefact_url, read_parquet_params)
    assert a_df_4.url == e_url
    # The expected df for the get has typed the partition columns as categories
    pd.testing.assert_frame_equal(e_df_2, a_df_4.dataframe.compute(), check_index_type=False)
    dd.utils.assert_eq(e_ddf, a_df_4.dataframe, check_divisions=False)

    # Check the correct parquet files have been created
    parquet_files = glob.glob(a_df.url + "/**/*.parquet", recursive=True)
    assert len(parquet_files) == 4
    expected_subdir = [
        "/year_filter=2021/month_filter=12/",
        "/year_filter=2022/month_filter=1/",
        "/year_filter=2022/month_filter=2/",
        "/year_filter=2022/month_filter=3/",
    ]
    for sub_dir in expected_subdir:
        matched_subdir = [f for f in parquet_files if sub_dir in f]
        assert len(matched_subdir) == 1

    artifacts.dask_dataframes.delete(a_df_2)

    with pytest.raises(FileNotFoundError):
        artifacts.dask_dataframes.get(a_df_2.url)
