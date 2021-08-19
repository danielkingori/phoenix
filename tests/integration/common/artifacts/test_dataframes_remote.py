"""Test of artifact dataframe functionality remotely.

Integration test that runs on a cloud provider.
"""
import pandas as pd
import pytest

from phoenix.common import constants
from phoenix.common.artifacts import dataframes


@pytest.mark.auth
def test_dataframe_artifact(tmp_s3_dir):
    """Test full functionality of DataFrame artifacts.

    This test uses a real S3 bucket to persist DataFrames to.
    """
    test_artefact_dir = tmp_s3_dir
    artefact_basename = "integration_df"
    artefact_url = dataframes.url(test_artefact_dir, artefact_basename)
    df = pd.DataFrame(
        {
            "A": [1],
        }
    )

    a_df = dataframes.persist(artefact_url, df)
    e_url = f"{test_artefact_dir}{artefact_basename}{constants.DATAFRAME_ARTIFACT_FILE_EXTENSION}"
    assert a_df.url == e_url
    pd.testing.assert_frame_equal(a_df.dataframe, df)

    a_df_2 = dataframes.get(artefact_url)
    assert a_df.url == a_df_2.url
    pd.testing.assert_frame_equal(a_df.dataframe, a_df_2.dataframe)

    dataframes.delete(a_df_2)

    with pytest.raises(Exception):
        dataframes.get(a_df_2.url)
