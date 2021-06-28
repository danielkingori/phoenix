"""Module to visualise posts by segmenting them through a Latent Dirichelet Allocation."""
import pandas as pd


def remove_links(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Removes links from a column."""
    df[col_name] = df[col_name].replace(to_replace=r"\S*https?:\S*", value="", regex=True)

    return df
