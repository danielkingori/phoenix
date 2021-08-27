"""ACLED event data.

See docs for more information: docs/acled_events.md.
"""
import pandas as pd
import tentaclio


def from_csvs(url_to_csv_folder: str) -> pd.DataFrame:
    """Get event data from folder of csvs."""
    li = []
    for entry in tentaclio.listdir(url_to_csv_folder):
        if entry == url_to_csv_folder[:-1]:
            continue
        with tentaclio.open(entry) as file_io:
            df = pd.read_csv(file_io, index_col=None, header=0)
            li.append(df)

    event_data = pd.concat(li, axis=0, ignore_index=True)
    return normalise(event_data)


def normalise(event_data: pd.DataFrame) -> pd.DataFrame:
    """Normalise and transform event data."""
    event_data["timestamp_filter"] = pd.to_datetime(
        event_data["event_date"], format="%d-%b-%y"
    ).dt.tz_localize("UTC")
    event_data["date_filter"] = event_data["timestamp_filter"].dt.date
    event_data["year_filter"] = event_data["timestamp_filter"].dt.year
    event_data["month_filter"] = event_data["timestamp_filter"].dt.month
    event_data["day_filter"] = event_data["timestamp_filter"].dt.day
    return event_data
