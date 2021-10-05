"""UNDP CSV format processing."""
import logging

import pandas as pd
import tentaclio

from phoenix.common import pd_utils


EVENT_DATE_FORMAT = "%d-%b-%y"


def from_csvs(url_to_csv_folder: str) -> pd.DataFrame:
    """Get event data from folder of csvs."""
    li = []
    for entry in tentaclio.listdir(url_to_csv_folder):
        if entry == url_to_csv_folder[:-1]:
            continue
        logging.info(f"Processing file: {entry}")
        with tentaclio.open(entry) as file_io:
            df = pd.read_csv(file_io, sep=";", index_col=None, header=0)
            li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    df = normalise(df)
    df = df.groupby("serial_number").last()
    df = df.reset_index()
    return df


def normalise(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalise and transform event data."""
    event_data = raw_df.rename(format_column, axis="columns")
    event_data["timestamp_filter"] = pd.to_datetime(
        event_data["incident_date"], format=EVENT_DATE_FORMAT
    ).dt.tz_localize("UTC")
    event_data["date_filter"] = event_data["timestamp_filter"].dt.date
    event_data["year_filter"] = event_data["timestamp_filter"].dt.year
    event_data["month_filter"] = event_data["timestamp_filter"].dt.month
    event_data["day_filter"] = event_data["timestamp_filter"].dt.day
    event_data["day"] = event_data["day"].astype("Int64")
    event_data["serial_number"] = event_data["serial_number"].astype("Int64")
    # These are type so that we check that the export is correct
    # In some what the validate that these columns are able to become ints
    # If they are not an error will be thrown
    # It is not perfect but is a simple and easy solution
    event_data["fatalities"] = event_data["fatalities"].astype("Int64")
    event_data["casualties"] = event_data["casualties"].astype("Int64")
    return event_data


def format_column(name: str):
    """Format UNDP event column."""
    result = name.replace("\\", " ")
    result = result.replace("-", "_")
    result = result.strip()
    return pd_utils.words_to_snake(result)
