"""Language sentiment dataset."""
import pandas as pd

from phoenix.common import artifacts, run_datetime


def persist(
    folder_url: str, df: pd.DataFrame, run_dt: run_datetime.RunDatetime
) -> artifacts.dtypes.ArtifactDataFrame:
    """Persist the language_sentiment dataset.

    Args:
        folder_url: the folder URL to persist the dataset.
        df: DataFrame to persist
        run_dt: RunDatetime that will be used to generate the file name.

    Returns:
        ArtifactDataFrame
    """
    file_url = f"{folder_url}{run_dt.to_file_safe_str()}.parquet"
    return artifacts.dataframes.persist(file_url, df)


def get(folder_url: str) -> pd.DataFrame:
    """Persist the language_sentiment dataset.

    Args:
        folder_url: the folder URL to get the dataset.

    Returns:
        DataFrame
    """
    art_ddf = artifacts.dask_dataframes.get(folder_url)
    return art_ddf.dataframe.compute()
