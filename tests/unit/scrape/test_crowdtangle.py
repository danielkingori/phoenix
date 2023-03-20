"""Test crowdtangle."""
import copy
import datetime
import json
import os

import mock
import pytest
import requests

from phoenix.common import constants, utils
from phoenix.scrape import crowdtangle


@pytest.fixture(scope="module")
def ct_data():
    """Crowdtangle mock data."""
    p = utils.relative_path("./crowdtangle_mock_data.json", __file__)
    with open(p) as json_file:
        return json.load(json_file)


@pytest.fixture()
def ct_data_with_next(ct_data):
    """Crowdtangle mock data with next page."""
    return ct_data


@pytest.fixture()
def ct_data_no_next(ct_data):
    """Crowdtangle mock data with no next page."""
    ct_no = copy.deepcopy(ct_data)
    ct_no["result"]["pagination"].pop("nextPage", None)
    return ct_no


@mock.patch.dict(os.environ, {"CT_RATE_LIMIT_CALLS": "10000"})
@mock.patch.dict(os.environ, {"CT_RATE_LIMIT_SECONDS": "1"})
@mock.patch("phoenix.scrape.crowdtangle.get_request_session")
@mock.patch("phoenix.scrape.crowdtangle.get_post")
def test_get_all_posts(m_get_post, m_get_request_session, ct_data_with_next, ct_data_no_next):
    """Get all posts for 2 pages."""
    m_get_post.side_effect = [
        ct_data_with_next,
        ct_data_no_next,
    ]
    mock_session = mock.MagicMock()
    m_get_request_session.return_value = mock_session

    start_date = datetime.datetime(2021, 1, 1)
    end_date = datetime.datetime(2021, 1, 1)
    list_ids = ["1"]
    posts = crowdtangle.get_all_posts(start_date, end_date, list_ids)

    calls = [
        mock.call(
            crowdtangle.POSTS_BASE_URL,
            {
                "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "listIds": list_ids,
                "sortBy": constants.FACEBOOK_POST_SORT_BY,
                "count": 100,
            },
            mock_session,
        ),
        mock.call(ct_data_with_next["result"]["pagination"]["nextPage"], {}, mock_session),
    ]

    m_get_post.assert_has_calls(calls)

    e_posts = ct_data_with_next["result"]["posts"] + ct_data_no_next["result"]["posts"]

    assert posts == e_posts


@mock.patch.dict(os.environ, {"CT_RATE_LIMIT_CALLS": "100"})
@mock.patch.dict(os.environ, {"CT_RATE_LIMIT_SECONDS": "300"})
def test_get_rate_limits_env():
    """Test get_rate_limits for set env variables."""
    rate_limit_calls, rate_limit_seconds = crowdtangle.get_rate_limits()
    assert rate_limit_calls == 100
    assert rate_limit_seconds == 300


@mock.patch.dict(os.environ, {"CT_RATE_LIMIT_CALLS": ""})
@mock.patch.dict(os.environ, {"CT_RATE_LIMIT_SECONDS": ""})
def test_get_rate_limits_default():
    """Test get_rate_limits for default values."""
    rate_limit_calls, rate_limit_seconds = crowdtangle.get_rate_limits()
    assert rate_limit_calls == 6
    assert rate_limit_seconds == 60


@pytest.mark.parametrize(
    "scrape_list_id_arg, expected_result",
    [
        ("id", ["id"]),
        ("id,id1", ["id", "id1"]),
    ],
)
def test_process_scrape_list_id(scrape_list_id_arg, expected_result):
    """Test process_scrape_list_id."""
    result = crowdtangle.process_scrape_list_id(scrape_list_id_arg)
    assert expected_result == result


def test_get_request_session():
    """Test that get_request_session returns an instatiated session."""
    session = crowdtangle.get_request_session()
    assert type(session) == requests.Session


@mock.patch("phoenix.scrape.crowdtangle.get_request_session")
@mock.patch("phoenix.scrape.crowdtangle.get_post")
def test_get_all_posts_connection_error(
    m_get_post, m_get_request_session, ct_data_with_next, ct_data_no_next
):
    """Get all posts still returns 2 pages when third call is a connection error."""
    m_get_post.side_effect = [
        ct_data_with_next,
        ct_data_with_next,
        requests.exceptions.ConnectionError(),
    ]
    mock_session = mock.MagicMock()
    m_get_request_session.return_value = mock_session

    start_date = datetime.datetime(2021, 1, 1)
    end_date = datetime.datetime(2021, 1, 1)
    list_ids = ["1"]
    posts = crowdtangle.get_all_posts(start_date, end_date, list_ids)

    calls = [
        mock.call(
            crowdtangle.POSTS_BASE_URL,
            {
                "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "listIds": list_ids,
                "sortBy": "total_interactions",
                "count": 100,
            },
            mock_session,
        ),
        mock.call(ct_data_with_next["result"]["pagination"]["nextPage"], {}, mock_session),
    ]

    m_get_post.assert_has_calls(calls)

    e_posts = ct_data_with_next["result"]["posts"] + ct_data_with_next["result"]["posts"]

    assert posts == e_posts
