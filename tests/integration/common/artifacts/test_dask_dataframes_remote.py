"""Dask artifacts DataFrame functionality test on a cloud provider."""
import pandas as pd
import pytest
from dask import dataframe as dd

from phoenix.common import artifacts


@pytest.mark.auth
def test_dataframe_artifact_partitions(tmp_s3_dir):
    """Test full functionality of Dask DataFrame artifacts when partitioned.

    This test uses a real S3 bucket to persist DataFrames to.
    """
    test_artefact_dir = tmp_s3_dir
    artefact_basename = "integration_df"
    artefact_url = f"{test_artefact_dir}{artefact_basename}"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "group": ["1", "1", "2", "2"],
        }
    )
    df["id"] = df["id"].astype("Int64")
    ddf = artifacts.utils.pandas_to_dask(df)

    a_df = artifacts.dask_dataframes.persist(artefact_url, ddf)
    e_url = f"{test_artefact_dir}{artefact_basename}"
    assert a_df.url == e_url
    pd.testing.assert_frame_equal(a_df.dataframe.compute(), df)

    a_df_2 = artifacts.dask_dataframes.get(artefact_url)
    assert a_df.url == a_df_2.url
    pd.testing.assert_frame_equal(a_df.dataframe.compute(), a_df_2.dataframe.compute())
    dd.utils.assert_eq(a_df.dataframe, a_df_2.dataframe)

    artifacts.dask_dataframes.delete(a_df_2)

    with pytest.raises(Exception):
        artifacts.dask_dataframes.get(a_df_2.url)
