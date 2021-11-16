"""Channels Config functionality."""
import pandas as pd
import tentaclio


def get_channels_to_scrape(config_url: str) -> pd.DataFrame:
    """Get the channel ids that are configured to scrape.

    Channel ids:
    https://stackoverflow.com/questions/14366648/how-can-i-get-a-channel-id-from-youtube

    Raises:
        RuntimeError: if there is no channel id:

    Returns:
        pandas DataFrame that has "channel_id" as a column of strings.
    """
    df = _get_channels_config(config_url)
    if "channel_id" not in df.columns:
        raise RuntimeError(
            "YouTube Channels config does not contain a channel_id."
            f" URL of config: {config_url}"
        )
    df["channel_id"] = df["channel_id"].astype("str")
    return df


def _get_channels_config(config_url: str) -> pd.DataFrame:
    """Get the channels config as a dataframe."""
    with tentaclio.open(config_url) as file_io:
        df = pd.read_csv(file_io)
    return df
