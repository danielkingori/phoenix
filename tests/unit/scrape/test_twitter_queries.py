"""Test the twitter query functionality."""
import os

import mock
import pytest

from phoenix.scrape import twitter_queries


@mock.patch.dict(
    os.environ,
    {
        twitter_queries.ENV_CONSUMER_KEY: "consumer_key",
        twitter_queries.ENV_CONSUMER_SECRET: "consumer_secret",
        twitter_queries.ENV_OAUTH_ACCESS_TOKEN: "oauth_access_token",
        twitter_queries.ENV_OAUTH_ACCESS_SECRET: "oauth_access_secret",
    },
)
@mock.patch("tweepy.OAuthHandler")
@mock.patch("tweepy.AppAuthHandler")
def test_get_auth_handler_oauth(m_AppAuthHandler, m_OAuthHandler):
    """Test the get_auth_handler is oauth."""
    auth = twitter_queries.get_auth_handler()
    assert auth == m_OAuthHandler.return_value
    m_OAuthHandler.assert_called_once_with("consumer_key", "consumer_secret")
    m_OAuthHandler.return_value.set_access_token.assert_called_once_with(
        "oauth_access_token",
        "oauth_access_secret",
    )
    m_AppAuthHandler.assert_not_called()


@mock.patch.dict(
    os.environ,
    {
        twitter_queries.ENV_CONSUMER_KEY: "consumer_key",
        twitter_queries.ENV_CONSUMER_SECRET: "consumer_secret",
        twitter_queries.ENV_OAUTH_ACCESS_TOKEN: "oauth_access_token",
        twitter_queries.ENV_OAUTH_ACCESS_SECRET: "oauth_access_secret",
        twitter_queries.ENV_APPLICATION_KEY: "applicaton_key",
        twitter_queries.ENV_APPLICATION_SECRET: "applicaton_secret",
    },
)
@mock.patch("tweepy.OAuthHandler")
@mock.patch("tweepy.AppAuthHandler")
def test_get_auth_handler_oauth_overrides(m_AppAuthHandler, m_OAuthHandler):
    """Test the get_auth_handler is oauth."""
    auth = twitter_queries.get_auth_handler()
    assert auth == m_OAuthHandler.return_value
    m_OAuthHandler.assert_called_once_with("consumer_key", "consumer_secret")
    m_OAuthHandler.return_value.set_access_token.assert_called_once_with(
        "oauth_access_token",
        "oauth_access_secret",
    )
    m_AppAuthHandler.assert_not_called()


@mock.patch.dict(
    os.environ,
    {
        twitter_queries.ENV_APPLICATION_KEY: "applicaton_key",
        twitter_queries.ENV_APPLICATION_SECRET: "applicaton_secret",
    },
)
@mock.patch("tweepy.OAuthHandler")
@mock.patch("tweepy.AppAuthHandler")
def test_get_auth_handler_application(m_AppAuthHandler, m_OAuthHandler):
    """Test the get_auth_handler is application."""
    auth = twitter_queries.get_auth_handler()
    assert auth == m_AppAuthHandler.return_value
    m_AppAuthHandler.assert_called_once_with("applicaton_key", "applicaton_secret")
    m_OAuthHandler.assert_not_called()


@mock.patch("tweepy.OAuthHandler")
@mock.patch("tweepy.AppAuthHandler")
def test_get_auth_handler_error(m_AppAuthHandler, m_OAuthHandler):
    """Test get_auth_handler with no enviroment keys."""
    with pytest.raises(RuntimeError) as error:
        twitter_queries.get_auth_handler()
        assert "not correct" in str(error.value)
    m_AppAuthHandler.assert_not_called()
    m_OAuthHandler.assert_not_called()
