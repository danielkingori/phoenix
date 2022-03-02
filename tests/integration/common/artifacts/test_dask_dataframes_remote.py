"""Dask artifacts DataFrame functionality test on a cloud provider."""
import pandas as pd
import pytest
from dask import dataframe as dd

from phoenix.common import artifacts
from tests.integration import conftest


def get_parquet_files(artifact_url):
    """Get the partquet files for the artifact."""
    offset = len(f"s3://{conftest.S3_INTEGRATION_BUCKET}/")
    prefix = artifact_url[offset:]
    all_files = conftest.get_s3_keys(conftest.S3_INTEGRATION_BUCKET, prefix)
    if len(all_files) > 0:
        return [f for f in all_files if f.endswith(".parquet")]

    return all_files


@pytest.mark.auth
def test_dataframe_artifact_partitions(tmp_s3_dir):
    """Test full functionality of Dask DataFrame artifacts when partitioned.

    This test uses a real S3 bucket to persist DataFrames to.
    """
    test_artefact_dir = tmp_s3_dir
    artefact_basename = "integration_df"
    artefact_url = f"{test_artefact_dir}{artefact_basename}"
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
    parquet_files = get_parquet_files(a_df.url)
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


@pytest.mark.auth
def test_dataframe_artifact_partition_overwrite(tmp_s3_dir):
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
    """
    test_artefact_dir = tmp_s3_dir
    artefact_basename = "integration_df"
    artefact_url = f"{test_artefact_dir}{artefact_basename}"
    to_parquet_params = {
        "engine": "pyarrow",
        "partition_on": ["year_filter", "month_filter"],
        "overwrite": False,
        "write_metadata_file": False,
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
    dd.utils.assert_eq(e_ddf, a_df_2.dataframe, check_divisions=False)

    # Check the correct parquet files have been created
    parquet_files = get_parquet_files(a_df.url)
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
            "id": [5, 6, 7, 8],
            "year_filter": [2022, 2022, 2022, 2022],
            "month_filter": [1, 1, 2, 3],
        }
    )

    df_2021 = df[df["year_filter"] == 2021]
    e_df_2 = pd.concat([df_2021, df_2.copy()])
    e_df_2["year_filter"] = e_df_2["year_filter"].astype("category")
    e_df_2["month_filter"] = e_df_2["month_filter"].astype("category")
    e_df_2["month_filter"] = e_df_2["month_filter"].astype("category")
    ddf_2 = artifacts.utils.pandas_to_dask(df_2)
    e_ddf = artifacts.utils.pandas_to_dask(e_df_2)

    a_df_3 = artifacts.dask_dataframes.persist(artefact_url, ddf_2, to_parquet_params)
    assert a_df_3.url == e_url
    # The original df is returned
    pd.testing.assert_frame_equal(a_df_3.dataframe.compute(), df_2)

    a_df_4 = artifacts.dask_dataframes.get(artefact_url, to_parquet_params)
    assert a_df_4.url == e_url
    # The expected df for the get has typed the partition columns as categories
    pd.testing.assert_frame_equal(e_df_2, a_df_4.dataframe.compute(), check_index_type=False)
    dd.utils.assert_eq(e_ddf, a_df_4.dataframe, check_divisions=False)

    # Check the correct parquet files have been created
    parquet_files = get_parquet_files(a_df.url)
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
