"""Tests for twitter."""
import datetime

import mock
import pandas as pd
import pytest

from phoenix.scrape import twitter_queries


@mock.patch("phoenix.scrape.twitter_queries.get_tweets")
@mock.patch("phoenix.scrape.twitter_queries.connect_twitter_api")
def test_get_tweets_dataframe(
    m_connect_twitter_api,
    m_get_tweets,
):
    """Tests correct get_user_tweets_dataframe."""
    # Args input
    query = ["keyword1", "keyword2"]
    num_items = 1
    since_days = 1
    query_type = "keywords"
    # API mocking
    tweet_mock = mock.Mock()
    mock_json = {"id": 1}
    tweet_mock._json = mock_json
    tweets = [tweet_mock, tweet_mock]
    m_get_tweets.return_value = tweets
    # Function called
    df = twitter_queries.get_tweets_dataframe(query_type, query, num_items, since_days)
    # Asserted behaviour
    m_connect_twitter_api.assert_called_once()
    m_get_tweets.assert_called_once_with(
        query_type, m_connect_twitter_api.return_value, query, num_items, since_days
    )
    # Asserted output
    pd.testing.assert_frame_equal(df, pd.DataFrame([mock_json, mock_json]))


@mock.patch("phoenix.scrape.twitter_queries.get_tweets_since_days")
def test_get_tweets(m_get_tweets_since_days):
    """Tests correct _get_tweets_for_ids."""
    # Args input
    query_type = "users"
    id_list = ["user1", "user2"]
    num_items = 1
    since_days = 1
    # API mocking
    api = mock.Mock()
    return_tweets = [mock.Mock(), mock.Mock()]
    m_get_tweets_since_days.return_value = return_tweets
    tweets = twitter_queries.get_tweets(query_type, api, id_list, num_items, since_days)
    calls = [
        mock.call(
            query_type,
            api,
            query=id_list[0],
            since_days=since_days,
            num_items=num_items,
        ),
        mock.call(query_type, api, query=id_list[1], since_days=since_days, num_items=num_items),
    ]
    # Asserted behavior
    m_get_tweets_since_days.assert_has_calls(calls)
    # Asserted output
    assert tweets == return_tweets * len(id_list)


@mock.patch("phoenix.scrape.twitter_queries._tweet_search_cursor")
@mock.patch("phoenix.scrape.twitter_queries._get_user_tweet_cursor")
def test_get_tweets_since_days(
    m_get_user_tweet_cursor,
    m_tweet_search_cursor,
):
    """Tests correct get_user_tweets_dataframe. Also proves twitter_utilities.is_recent_tweet."""
    # Args input
    query_type = "users"
    query = "user1"
    num_items = 1
    since_days = 1
    # API mocking
    api = mock.Mock()
    # Set passing and failing date, and set to query results
    passing_date = datetime.datetime.now()
    failing_date = passing_date - datetime.timedelta(since_days + 1)
    query_result = [mock.Mock(created_at=passing_date), mock.Mock(created_at=failing_date)]
    m_get_user_tweet_cursor.return_value = query_result
    # Function setup and call
    calls = []
    tweets = {}
    for status in twitter_queries.get_tweets_since_days(
        query_type=query_type,
        api=api,
        query=query,
        num_items=num_items,
        since_days=since_days,
    ):
        tweets = status
        calls.extend(mock.call(api=api, query=query, num_items=num_items))
    # Asserted behavior
    m_get_user_tweet_cursor.assert_has_calls(calls[0])
    m_tweet_search_cursor.assert_not_called()
    # Asserted output
    assert tweets == query_result[0]


def test_get_tweets_since_days_fails_with_bad_query_type():
    """Tests incorrect query type for get_tweets_since_days"""
    bad_query_type = "bad query type"
    query = "bad query"
    num_items = 1
    since_days = 1
    # API mocking
    api = mock.Mock()
    # Assert behavior
    with pytest.raises(ValueError):
        tweets = twitter_queries.get_tweets_since_days(
            bad_query_type, query, num_items, since_days, api=api
        )
        next(tweets)
