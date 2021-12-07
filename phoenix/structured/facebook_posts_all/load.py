"""Load of the facebook_posts_all."""
import pandas as pd

from phoenix.common import artifacts


def execute(
    facebook_posts_all_url: str, df: pd.DataFrame
) -> artifacts.dtypes.ArtifactDaskDataFrame:
    """Persist the dataframe for facebook_posts_all."""
    return artifacts.dask_dataframes.persist(
        facebook_posts_all_url,
        artifacts.utils.pandas_to_dask(df),
        partition_cols=["year_filter", "month_filter", "day_filter"],
        to_parquet_params={
            "append": True,
            "compute": True,
        },
    )
