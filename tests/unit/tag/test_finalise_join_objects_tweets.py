"""Test finalise functionality for tweets."""
import pandas as pd
import pytest

from phoenix.tag import finalise


@pytest.fixture
def tweets_for_join():
    """Tweets for the join."""
    return pd.DataFrame(
        {
            "id_str": ["1", "2", "3", "4"],
            "object_type": ["ot", "ot", "ot", "ot"],
            "url": ["url1", "url2", "url3", "url4"],
            "year_filter": [2021, 2021, 2021, 2021],
            "month_filter": [1, 1, 2, 2],
        }
    )


@pytest.fixture
def objects_for_join():
    """Objects for the join."""
    return pd.DataFrame(
        {
            "object_id": ["1", "2", "3", "4"],
            "topic": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
            "has_topic": [True, True, False, True],
            "retweeted": [False, False, True, True],
            "text": ["text_1", "text_2", "text_3", "text_4"],
            "language_from_api": ["ar", "en", "en", "ar"],
        }
    )


@pytest.fixture
def language_sentiment_objects_for_join():
    """Language Sentiment Objects for the join."""
    return pd.DataFrame(
        {
            "object_id": ["1", "2", "3", "4"],
            "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
            "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
        }
    )


def test_join_object_to_tweets(
    tweets_for_join, objects_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to tweets."""
    result_df = finalise.join_objects_to_tweets(
        tweets=tweets_for_join,
        objects=objects_for_join,
        language_sentiment_objects=language_sentiment_objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id_str": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "topic": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_topic": [True, True, False, True],
                "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
                "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )


def test_join_object_to_tweets_none_objects_to_join(
    tweets_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_tweets(
        tweets=tweets_for_join,
        language_sentiment_objects=language_sentiment_objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id_str": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
                "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )


def test_join_object_to_tweets_none_langauge_sentiment_objects(tweets_for_join, objects_for_join):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_tweets(
        tweets=tweets_for_join,
        objects=objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id_str": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "topic": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_topic": [True, True, False, True],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )


def test_join_object_to_tweets_none(tweets_for_join):
    """Test the join of objects to tweets."""
    result_df = finalise.join_objects_to_tweets(
        tweets=tweets_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id_str": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )
