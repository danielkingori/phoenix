"""Test Group By functionality."""
from typing import get_args

import datetime

import mock
import pytest

from phoenix.common import artifacts
from phoenix.scrape import group_by


@pytest.mark.parametrize(
    "objects_scraped_since, objects_scraped_till, expected_list",
    [
        (
            datetime.datetime(2001, 12, 31),
            datetime.datetime(2002, 2, 1),
            [
                group_by.GroupFilters(2001, 12),
                group_by.GroupFilters(2002, 1),
                group_by.GroupFilters(2002, 2),
            ],
        ),
        (
            datetime.datetime(2001, 12, 1),
            datetime.datetime(2001, 12, 2),
            [
                group_by.GroupFilters(2001, 12),
            ],
        ),
    ],
)
def test_get_group_filters(objects_scraped_since, objects_scraped_till, expected_list):
    """Test get_group_filters."""
    assert expected_list == list(
        group_by.get_group_filters(objects_scraped_since, objects_scraped_till)
    )


@mock.patch("phoenix.scrape.group_by.get_group_filters")
@mock.patch("phoenix.common.artifacts.json.persist")
def test_persist(m_json_persist, m_get_group_filter):
    """Test the persist group by."""
    group_filters = [
        group_by.GroupFilters(2001, 12),
        group_by.GroupFilters(2002, 1),
        group_by.GroupFilters(2002, 2),
    ]

    m_get_group_filter.return_value = group_filters

    start_dt = mock.MagicMock(spec=datetime.datetime)
    end_dt = mock.MagicMock(spec=datetime.datetime)
    arch_reg = mock.MagicMock(artifacts.registry.ArtifactURLRegistry)
    artifacts_key = get_args(artifacts.registry_mappers.ArtifactKey)[0]
    objects = [{"objects": "objects"}]
    group_by.persist(arch_reg, artifacts_key, objects, start_dt, end_dt)
    m_get_group_filter.assert_called_once_with(start_dt, end_dt)

    calls = [
        mock.call(artifacts_key, {"YEAR_FILTER": 2001, "MONTH_FILTER": 12}),
        mock.call(artifacts_key, {"YEAR_FILTER": 2002, "MONTH_FILTER": 1}),
        mock.call(artifacts_key, {"YEAR_FILTER": 2002, "MONTH_FILTER": 2}),
    ]
    arch_reg.get_url.assert_has_calls(calls)
    persist_calls = [mock.call(arch_reg.get_url.return_value, objects)] * 3
    m_json_persist.assert_has_calls(persist_calls)


@mock.patch("phoenix.scrape.group_by.persist")
def test_persist_facebook_posts(m_persist):
    """Test persist facebook_posts."""
    start_dt = mock.MagicMock(spec=datetime.datetime)
    end_dt = mock.MagicMock(spec=datetime.datetime)
    arch_reg = mock.MagicMock(spec=artifacts.registry.ArtifactURLRegistry)
    objects = [{"objects": "objects"}]
    group_by.persist_facebook_posts(arch_reg, objects, start_dt, end_dt)
    expected_artifact_key: artifacts.registry_mappers.ArtifactKey = "base-grouped_by_posts"
    m_persist.assert_called_once_with(arch_reg, expected_artifact_key, objects, start_dt, end_dt)
