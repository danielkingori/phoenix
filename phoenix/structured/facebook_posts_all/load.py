"""Load of the facebook_posts_all."""
import pandas as pd
import prefect

from phoenix.common import artifacts


@prefect.task
def load_task(
    facebook_posts_all_url: str, df: pd.DataFrame
) -> artifacts.dtypes.ArtifactDaskDataFrame:
    """Task for the load."""
    return execute(facebook_posts_all_url, df)


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
            "ignore_divisions": True,
        },
    )
