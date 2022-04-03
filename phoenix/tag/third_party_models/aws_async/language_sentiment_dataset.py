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
    # Deduplication in case there is duplicate persisted
    # This could happen if there are two cuncurrent start and complete sentiment
    # scripts run.
    # This is race condition will only cause extra costs so it not
    # a huge problem
    df = art_ddf.dataframe.drop_duplicates(subset=["object_id"])
    return df.compute()


def get_objects_still_to_analyse(objects_df: pd.DataFrame, language_sentiment_dataset_url: str):
    """Get the objects that are have not yet been analysed.

    Args:
        objects_url: the url of the objects url.
        language_sentiment_dataset_url: the url of the language_sentiment_dataset

    Returns:
        DataFrame
    """
    language_sentiment_df = get(language_sentiment_dataset_url)
    matched_df = language_sentiment_df[objects_df.columns]
    result_df = pd.concat([objects_df, matched_df]).drop_duplicates(
        subset=["object_id"], keep=False
    )
    return result_df.reset_index(drop=True)
