"""Unit tests of DataFrame artifacts."""
import mock
import pandas as pd
import pytest

from phoenix.common.artifacts import dataframes


@pytest.mark.parametrize(
    "artifacts_directory, artifact_basename, result",
    [
        ("dir/", "base", "dir/base.parquet"),
        ("dir/", "base.parquet", "dir/base.parquet"),
    ],
)
def test_url_creation(artifacts_directory, artifact_basename, result):
    """Test creation of DataFrame URL."""
    assert result == dataframes.url(artifacts_directory, artifact_basename)


@mock.patch("phoenix.common.artifacts.dataframes._persist")
def test_persist_error(m__persist):
    """Test persist of ArtefactDataFrame throws error on invalid URL."""
    url = "dir/df.invalid"
    dataframe = pd.DataFrame({"A": [1]})
    with pytest.raises(ValueError):
        dataframes.persist(url, dataframe)

    m__persist.assert_not_called()


@mock.patch("phoenix.common.artifacts.dataframes._persist")
def test_persist_dataframe_copy(m__persist):
    """Test persist copies the dataframe to when creating ArtefactDataFrame."""
    url = "dir/df.parquet"
    dataframe = pd.DataFrame({"A": [1]})
    a_df = dataframes.persist(artifacts_dataframe_url=url, dataframe=dataframe)
    m__persist.assert_called_once_with(a_df)
    assert a_df.url == url
    # Checking that the dataframe was copied but they are equal
    assert a_df.dataframe is not dataframe
    pd.testing.assert_frame_equal(a_df.dataframe, dataframe)
