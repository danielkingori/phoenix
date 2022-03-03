"""Final Datasets."""
from typing import List, Optional

import pandas as pd

from phoenix.common import artifacts


DEFAULT_PARTITION_ON = ["year_filter", "month_filter", "day_filter"]


def persist(
    artifact_url: str, df: pd.DataFrame, partition_on: Optional[List[str]] = None
) -> artifacts.dtypes.ArtifactDaskDataFrame:
    """Persist the final datasets as partitioned dataset.

    This is also configured to overwrite the partitions dynamically.
    Meaning all the partitions that are included in the given dataframe
    will be overwritten. Partitions not included in the dataset will be
    left.

    For an further explanation of this see:
    tests/integration/common/artifacts/test_dask_dataframes_local.py

    This uses a hack to overwrite a single dataframe partition.
    This means the distributed nature of dask is not used.
    """
    # This is a hack so we can only do this with 1 partition
    ddf = artifacts.utils.pandas_to_dask(df, 1)

    if not partition_on:
        partition_on = DEFAULT_PARTITION_ON

    to_parquet_params = {
        "engine": "pyarrow",
        "partition_on": partition_on,
        "overwrite": False,
        "write_metadata_file": False,
        "write_index": False,
    }
    return artifacts.dask_dataframes.persist(artifact_url, ddf, to_parquet_params)


def read(
    artifact_url: str, partition_on: Optional[List[str]] = None
) -> artifacts.dtypes.ArtifactDaskDataFrame:
    """Read the final datasets as partitioned dataset."""
    if not partition_on:
        partition_on = DEFAULT_PARTITION_ON

    read_parquet_params = {"engine": "pyarrow", "index": partition_on}
    return artifacts.dask_dataframes.get(artifact_url, read_parquet_params)
