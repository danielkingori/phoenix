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
