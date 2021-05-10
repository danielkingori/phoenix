"""Tests for twitter."""
import mock
import pandas as pd
import tweepy

from phoenix.twitter import twitter_queries


@mock.patch("phoenix.twitter.twitter_queries._get_tweets_for_ids")
@mock.patch("phoenix.twitter.twitter_queries.connect_twitter_api")
def test_get_user_tweets_dataframe(
    m_connect_twitter_api,
    m_get_tweets_for_ids,
):
    """Tests correct get_user_tweets_dataframe."""
    # Args input
    id_list = ["user1", "user2"]
    num_items = 1
    since_days = 1
    # API mocking
    tweet_mock = mock.Mock()
    mock_json = {"id": 1}
    tweet_mock._json = mock_json
    tweets = [tweet_mock, tweet_mock]
    m_get_tweets_for_ids.return_value = tweets
    # Function called
    df = twitter_queries.get_user_tweets_dataframe(id_list, num_items, since_days)
    # Asserted behaviour
    m_connect_twitter_api.assert_called_once()
    m_get_tweets_for_ids.assert_called_once_with(
        m_connect_twitter_api.return_value, id_list, num_items, since_days
    )
    # Asserted output
    pd.testing.assert_frame_equal(df, pd.DataFrame([mock_json, mock_json]))


