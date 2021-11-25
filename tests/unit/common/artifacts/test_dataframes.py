"""Unit tests of DataFrame artifacts."""
import mock
import pandas as pd
import pytest
import tentaclio

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
    m__persist.assert_called_once_with(a_df, {})
    assert a_df.url == url
    # Checking that the dataframe was copied but they are equal
    assert a_df.dataframe is not dataframe
    pd.testing.assert_frame_equal(a_df.dataframe, dataframe)


@mock.patch("phoenix.common.artifacts.dataframes._persist")
def test_persist_dataframe_copy_to_parquet_params(m__persist):
    """Test persist copies the dataframe to when creating ArtefactDataFrame."""
    url = "dir/df.parquet"
    dataframe = pd.DataFrame({"A": [1]})
    to_parquet_params = {"p": 1}
    a_df = dataframes.persist(
        artifacts_dataframe_url=url, dataframe=dataframe, to_parquet_params=to_parquet_params
    )
    m__persist.assert_called_once_with(a_df, to_parquet_params)
    assert a_df.url == url
    # Checking that the dataframe was copied but they are equal
    assert a_df.dataframe is not dataframe
    pd.testing.assert_frame_equal(a_df.dataframe, dataframe)


@mock.patch("phoenix.common.artifacts.dataframes.get")
def test_get_dataframe(m_get):
    """Test get_dataframe."""
    url = "url"
    result = dataframes.get_dataframe(url)
    m_get.assert_called_once_with(url)
    assert result == m_get().dataframe


@mock.patch("phoenix.common.artifacts.dataframes.get")
def test_get_dataframe_return_none(m_get):
    """Test get_dataframe with `allow_not_found`."""
    url = "url"
    m_get.side_effect = FileNotFoundError(url)
    result = dataframes.get_dataframe(url)
    m_get.assert_called_once_with(url)
    assert result is None


@mock.patch("phoenix.common.artifacts.dataframes.get")
def test_get_dataframe_none_client_error(m_get):
    """Test get_dataframe with `allow_not_found` with a client error."""
    url = "url"
    m_get.side_effect = tentaclio.clients.exceptions.ClientError(url)
    result = dataframes.get_dataframe(url)
    m_get.assert_called_once_with(url)
    assert result is None


@mock.patch("phoenix.common.artifacts.dataframes.get")
def test_get_dataframe_raises_error(m_get):
    """Test get_dataframe_for_url with error and `allow_not_found` is False."""
    url = "url"
    m_get.side_effect = FileNotFoundError(url)
    with pytest.raises(FileNotFoundError):
        dataframes.get_dataframe(url, allow_not_found=False)
        m_get.assert_called_once_with(url)
