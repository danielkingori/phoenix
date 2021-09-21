"""ACLED event data.

See docs for more information: docs/acled_events.md.
"""
import logging

import pandas as pd
import tentaclio


# Not sure as to why to format of the raw data is like this
# The documentation is not great
# Quote:
#  What is the date format for ACLED events?
#  The data use a dd-mm-yyyy format (UK style) and for all future iterations of the dataset,
#  the format will be consistently a dd-mmmm-yyyy format to alleviate any problems.
# However this is not the format that was in the raw download
EVENT_DATE_FORMAT = "%d %B %Y"


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
    df = df.sort_values("updated_at")
    df = df.groupby("data_id").last()
    df = df.reset_index()
    return df


def normalise(event_data: pd.DataFrame) -> pd.DataFrame:
    """Normalise and transform event data."""
    event_data["timestamp_filter"] = pd.to_datetime(
        event_data["event_date"], format=EVENT_DATE_FORMAT
    ).dt.tz_localize("UTC")
    event_data["date_filter"] = event_data["timestamp_filter"].dt.date
    event_data["year_filter"] = event_data["timestamp_filter"].dt.year
    event_data["month_filter"] = event_data["timestamp_filter"].dt.month
    event_data["day_filter"] = event_data["timestamp_filter"].dt.day
    event_data["updated_at"] = get_updated_at(event_data)
    event_data["data_id"] = event_data["data_id"].astype("Int64")
    return event_data


def get_updated_at(event_data: pd.DataFrame):
    """Get the updated at."""
    if "timestamp" not in event_data.columns:
        raise ValueError("Event data is unknown format: has no `timestamp` column.")

    return pd.to_datetime(event_data["timestamp"], unit="s").dt.tz_localize("UTC")
