"""Test of the tagging export."""
import mock
import numpy as np
import pandas as pd

from phoenix.tag import constants, export


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


def create_test_posts_df(number_of_rows: int = 100) -> pd.DataFrame:
    """Creates posts_df with test values."""
    post = {
        "account_name": "name",
        "post_created": "post_created",
        "text": "text",
        "post_url": "post_url",
        "scrape_url": "scrape_url",
        "some_extra_col": "some_extra_col",
    }
    result_data = [post] * number_of_rows
    result_df = pd.DataFrame(result_data)
    result_df["phoenix_post_id"] = np.arange(result_df.shape[0])
    result_df["total_interactions"] = np.arange(result_df.shape[0])
    result_df = result_df.sort_values(by="total_interactions", ascending=False)
    return result_df


def test_get_posts_to_scrape_default_percent():
    """Test posts to scrape default percent."""
    posts_df = create_test_posts_df()
    result_df = export.get_posts_to_scrape(posts_df)
    expected_df = posts_df[:10]
    expected_df = expected_df[export.POSTS_TO_SCRAPE_COLUMNS]
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_get_posts_to_scrape_set_percent():
    """Test posts to scrape are correct with percentage_of_posts is set."""
    posts_df = create_test_posts_df()
    result_df = export.get_posts_to_scrape(posts_df, 20)
    expected_df = posts_df[:20]
    expected_df = expected_df[export.POSTS_TO_SCRAPE_COLUMNS]
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_get_posts_to_scrape_max_capped():
    """Test get_posts_to_scrape returns the max cap number of posts.

    This happens when 10% of the posts_df is > max cap.
    """
    total_posts = constants.TO_LABEL_CSV_MAX * 20
    posts_df = create_test_posts_df(total_posts)
    result_df = export.get_posts_to_scrape(posts_df)
    expected_df = posts_df[: constants.TO_LABEL_CSV_MAX]
    expected_df = expected_df[export.POSTS_TO_SCRAPE_COLUMNS]
    pd.testing.assert_frame_equal(result_df, expected_df)
