"""Artifacts DataFrame functionality test on the local file system.

Tests in this module are testing the full functionality of the DataFrame
artifacts on the local file system.
"""
import pandas as pd
import pytest

from phoenix.common import artifacts, constants


def test_dataframe_artifact(tmp_path):
    """Test full functionality of DataFrame artifacts."""
    test_artefact_dir = tmp_path / "dataframe_artifacts/"
    artefact_basename = "df"
    artefact_url = artifacts.dataframes.url(test_artefact_dir, artefact_basename)
    df = pd.DataFrame(
        {
            "A": [1],
        }
    )

    a_df = artifacts.dataframes.persist(artefact_url, df)
    e_url = f"{test_artefact_dir}{artefact_basename}{constants.DATAFRAME_ARTIFACT_FILE_EXTENSION}"
    assert a_df.url == e_url
    pd.testing.assert_frame_equal(a_df.dataframe, df)

    a_df_2 = artifacts.dataframes.get(artefact_url)
    assert a_df.url == a_df_2.url
    pd.testing.assert_frame_equal(a_df.dataframe, a_df_2.dataframe)

    artifacts.dataframes.delete(a_df_2)

    with pytest.raises(FileNotFoundError):
        artifacts.dataframes.get(a_df_2.url)
