"""Test Artifacts Utils."""
import os

import mock
import pandas as pd
import pytest

from phoenix.common import artifacts


@pytest.mark.parametrize(
    "npartitions,default_npartitions,expected_npartitions",
    [(None, None, 1), (None, "10", 10), (10, None, 10), (10, "11", 10)],
)
@mock.patch("dask.dataframe.from_pandas")
def test_pandas_to_dask_mocked(
    m_from_pandas, npartitions, default_npartitions, expected_npartitions
):
    """Test pandas_to_dask functionality.

    The number of assigned npartitions is the max number of partitions so a mock
    of `from_pandas` is being used.
    """
    df = pd.DataFrame(
        {
            "A": [range(0, 1000, 1)],
            "B": [range(0, 1000, 1)],
        }
    )
    df = df.set_index("A")

    env_name = "DEFAULT_NPARTITIONS"

    if default_npartitions:
        with mock.patch.dict(os.environ, {env_name: default_npartitions}):
            ddf = artifacts.utils.pandas_to_dask(df, npartitions)
    else:
        ddf = artifacts.utils.pandas_to_dask(df, npartitions)

    m_from_pandas.assert_called_once_with(df, npartitions=expected_npartitions)
    assert ddf == m_from_pandas.return_value


@pytest.mark.parametrize(
    "npartitions,default_npartitions,expected_npartitions",
    [(None, None, 1), (None, "10", 10), (10, None, 10), (10, "11", 10)],
)
def test_pandas_to_dask(npartitions, default_npartitions, expected_npartitions):
    """Test pandas_to_dask functionality."""
    df = pd.DataFrame(
        {
            "A": [range(0, 1000, 1)],
            "B": [range(0, 1000, 1)],
        }
    )
    df = df.set_index("A")

    env_name = "DEFAULT_NPARTITIONS"

    if default_npartitions:
        with mock.patch.dict(os.environ, {env_name: default_npartitions}):
            ddf = artifacts.utils.pandas_to_dask(df, npartitions)
    else:
        ddf = artifacts.utils.pandas_to_dask(df, npartitions)

    assert ddf.columns.all(df.columns)
    assert ddf.npartitions <= expected_npartitions
