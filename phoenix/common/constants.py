"""Constants for Phoenix."""
from typing import Literal


# Artifacts
DATAFRAME_ARTIFACT_FILE_EXTENSION = ".parquet"

# Facebook API sort by/score value
# This will be used when mapping the
# source data
FACEBOOK_POST_SORT_BY = "total_interactions"


SCRAPE_RUN_TWEET_TYPES = Literal["user", "keyword"]
