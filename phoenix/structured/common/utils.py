"""Structured Utils."""
import pandas as pd

from phoenix.common.artifacts import source_file_name_processing


def get_source_file_name(file_url) -> source_file_name_processing.SourceFileName:
    """Get a SourceFileName name from the file URL.

    Args:
        file_url (str): URL of the file.

    Raises:
        RuntimeError if a SourceFileName can't be computed for the file url

    Returns
        SourceFileName
    """
    source_file_name = source_file_name_processing.get_source_file_name(file_url)
    if not source_file_name:
        raise RuntimeError(
            (
                f"Unable to process file name {file_url}."
                " You may need to add a timestamp to the file name."
                " Check phoenix/common/run_datetime.py for format."
            )
        )
    return source_file_name


def add_file_cols(
    df: pd.DataFrame, source_file_name: source_file_name_processing.SourceFileName
) -> pd.DataFrame:
    """Add the file data columns.

    Args:
        df (pd.DataFrame): A DataFrame
        source_file_name (SourceFileName): A SourceFileName object

    Returns:
        Pandas DataFrame with:
            file_timestamp (datetime UTC): that was in the file name
            file_url (str): full file url
            file_basename (str): base name of the file
    """
    file_timestamp = source_file_name.run_dt.dt
    df["file_timestamp"] = file_timestamp
    df["file_url"] = source_file_name.full_url
    df["file_base"] = source_file_name.get_basename()
    return df


def add_filter_cols(df: pd.DataFrame, created_at_col: pd.Series) -> pd.DataFrame:
    """Add the filter columns based on the created at col.

    Adds columns:
        - "timestamp_filter"
        - "date_filter"
        - "year_filter"
        - "month_filter"
        - "day_filter"

    Returns:
        New dataframe with the filter columns.
    """
    df = df.copy()
    df["timestamp_filter"] = created_at_col
    df["date_filter"] = created_at_col.dt.date
    df["year_filter"] = created_at_col.dt.year
    df["month_filter"] = created_at_col.dt.month
    df["day_filter"] = created_at_col.dt.day
    return df
