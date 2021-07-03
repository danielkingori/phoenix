"""Test of artifact dataframe functionality."""
import datetime

import pandas as pd
import pytest

from phoenix.common.artifacts import dataframes


def test_read_schema(tmp_path):
    """Test the read schema.

    Persist a dataframe.

    Read is schema.
    """
    url = f"file:///{tmp_path}/persist.parquet"
    df = pd.DataFrame(
        {
            "int64": [0, 1],
            "double": [0.0, 1.1],
            "timestamp[us]": [
                datetime.datetime.utcnow(),
                datetime.datetime.fromisoformat("2011-11-04 00:05:23.283+00:00"),
            ],
            "string": ["str", "str2"],
        }
    )

    dataframes.persist(url, df)
    schema = dataframes.read_schema(url)

    pd.testing.assert_frame_equal(
        schema,
        pd.DataFrame(
            {
                "column": ["int64", "double", "timestamp[us]", "string"],
                "pa_dtype": ["int64", "double", "timestamp[us]", "string"],
            }
        ),
    )


def test_not_found_read_schema(tmp_path):
    """Test the read schema not found."""
    url = f"file:///{tmp_path}/persist.parquet"
    with pytest.raises(FileNotFoundError):
        dataframes.read_schema(url)
