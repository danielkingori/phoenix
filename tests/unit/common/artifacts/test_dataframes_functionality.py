"""Artefacts DataFrame functionality test.

Tests in this module are testing the full functionality of the DataFrame
artifacts. They are not strictly unit test however they are here because they
are integrating with the file system rather then some Google Cloud Platform
resource.

The idea is that if they integrate with the file system they will be able
to work with a remote URL since we use tentaclio to homogenize
the system and cloud provider file functionality.
"""
import pandas as pd
import pytest

from phoenix.common import artifacts, constants


def test_dataframe_artifact(tmp_path):
    """Test full functionality of DataFrame artifacts."""
    test_artifact_dir = tmp_path / "dataframe_artifacts/"
    artifact_basename = "df"
    artifact_url = artifacts.dataframes.url(test_artifact_dir, artifact_basename)
    df = pd.DataFrame(
        {
            "A": [1],
        }
    )

    a_df = artifacts.dataframes.persist(artifact_url, df)
    e_url = f"{test_artifact_dir}{artifact_basename}{constants.DATAFRAME_ARTIFACT_FILE_EXTENSION}"
    assert a_df.url == e_url
    pd.testing.assert_frame_equal(a_df.dataframe, df)

    a_df_2 = artifacts.dataframes.get(artifact_url)
    assert a_df.url == a_df_2.url
    pd.testing.assert_frame_equal(a_df.dataframe, a_df_2.dataframe)

    artifacts.dataframes.delete(a_df_2)

    with pytest.raises(FileNotFoundError):
        artifacts.dataframes.get(a_df_2.url)
