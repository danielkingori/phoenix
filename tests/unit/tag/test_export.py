"""Test of the tagging export."""
import mock
import pandas as pd

from phoenix.tag import export


@mock.patch("tentaclio.open")
def test_persist_posts_to_scrape_no_dashboard_url(m_open):
    """Test the persist_posts_to_scrape when no dashboard_url."""
    posts_to_scrape = mock.MagicMock(spec=pd.DataFrame)
    tagging_url = "tag.csv"
    export.persist_posts_to_scrape(posts_to_scrape, tagging_url)
    calls = [
        mock.call(tagging_url, "w"),
        mock.call().__enter__(),
        mock.call().__exit__(None, None, None),
    ]
    m_open.assert_has_calls(calls)
    calls = [mock.call(m_open().__enter__())]
    posts_to_scrape.to_csv.assert_has_calls(calls)


@mock.patch("tentaclio.open")
def test_persist_posts_to_scrape_dashboard_url(m_open):
    """Test the persist_posts_to_scrape when dashboard_url."""
    posts_to_scrape = mock.MagicMock(spec=pd.DataFrame)
    tagging_url = "tag.csv"
    dashboard_url = "dashboard.csv"
    export.persist_posts_to_scrape(posts_to_scrape, tagging_url, dashboard_url)
    calls = [
        mock.call(tagging_url, "w"),
        mock.call().__enter__(),
        mock.call().__exit__(None, None, None),
        mock.call(dashboard_url, "w"),
        mock.call().__enter__(),
        mock.call().__exit__(None, None, None),
    ]
    m_open.assert_has_calls(calls)
    calls = [mock.call(m_open().__enter__()), mock.call(m_open().__enter__())]
    posts_to_scrape.to_csv.assert_has_calls(calls)
