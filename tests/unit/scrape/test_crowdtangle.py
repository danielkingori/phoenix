"""Test crowdtangle."""
import copy
import datetime
import json

import mock
import pytest

from phoenix.common import utils
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


@mock.patch("phoenix.scrape.crowdtangle.single_get")
def test_get_all_posts(m_single_get, ct_data_with_next, ct_data_no_next):
    """Get all posts for 2 pages."""
    m_single_get.side_effect = [
        ct_data_with_next,
        ct_data_no_next,
    ]

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
        ),
        mock.call(ct_data_with_next["result"]["pagination"]["nextPage"], {}),
    ]

    m_single_get.assert_has_calls(calls)

    e_posts = ct_data_with_next["result"]["posts"] + ct_data_no_next["result"]["posts"]

    assert posts == e_posts
