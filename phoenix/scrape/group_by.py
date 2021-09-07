"""Group source by functionality."""
from typing import Iterator, List, Optional

import dataclasses
import datetime

from phoenix.common import artifacts, constants, run_datetime


@dataclasses.dataclass
class GroupFilters:
    """Group Filters that will be used to group the objects."""

    YEAR_FILTER: int
    MONTH_FILTER: int


def get_group_filters(
    objects_scraped_since: datetime.datetime, objects_scraped_till: datetime.datetime
) -> Iterator[GroupFilters]:
    """Get months and years between the datetimes."""
    if objects_scraped_since > objects_scraped_till:
        raise ValueError(
            f"Start date {objects_scraped_since} is not before end date {objects_scraped_till}"
        )

    year = objects_scraped_since.year
    month = objects_scraped_since.month

    while (year, month) <= (objects_scraped_till.year, objects_scraped_till.month):
        yield GroupFilters(year, month)

        # Move to the next month.  If we're at the end of the year, wrap around
        # to the start of the next.
        #
        # Example: Nov 2017
        #       -> Dec 2017 (month += 1)
        #       -> Jan 2018 (end of year, month = 1, year += 1)
        #
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    return []


def persist(
    arch_reg: artifacts.registry.ArtifactURLRegistry,
    artifacts_key: artifacts.registry_mappers.ArtifactKey,
    objects,
    objects_scraped_since: datetime.datetime,
    objects_scraped_till: datetime.datetime,
) -> List[artifacts.dtypes.ArtifactJson]:
    """Persist grouping by."""
    group_filters = get_group_filters(objects_scraped_since, objects_scraped_till)
    artifacts_json: List[artifacts.dtypes.ArtifactJson] = []
    for group_filter in group_filters:
        url = arch_reg.get_url(artifacts_key, dataclasses.asdict(group_filter))
        artifacts_json.append(artifacts.json.persist(url, objects))
    return artifacts_json


def persist_facebook_posts(
    arch_reg: artifacts.registry.ArtifactURLRegistry,
    objects,
    objects_scraped_since: datetime.datetime,
    objects_scraped_till: datetime.datetime,
) -> List[artifacts.dtypes.ArtifactJson]:
    """Persist grouping by for facebook posts."""
    artifacts_key: artifacts.registry_mappers.ArtifactKey = "base-grouped_by_posts"
    return persist(arch_reg, artifacts_key, objects, objects_scraped_since, objects_scraped_till)


def persist_tweets(
    arch_reg: artifacts.registry.ArtifactURLRegistry,
    tweets_type: constants.SCRAPE_RUN_TWEET_TYPES,
    objects,
    run_dt: run_datetime.RunDatetime,
    scrape_since_days: int,
) -> List[artifacts.dtypes.ArtifactJson]:
    """Persist grouping by for tweets."""
    artifacts_key: Optional[artifacts.registry_mappers.ArtifactKey] = None
    if tweets_type == "user":
        artifacts_key = "base-grouped_by_user_tweets"

    if tweets_type == "keyword":
        artifacts_key = "base-grouped_by_keyword_tweets"

    if artifacts_key is None:
        raise RuntimeError(f"Type of tweets is not supported {tweets_type}")

    objects_scraped_till = run_dt.dt
    objects_scraped_since = run_dt.dt - datetime.timedelta(days=scrape_since_days)
    return persist(arch_reg, artifacts_key, objects, objects_scraped_since, objects_scraped_till)
