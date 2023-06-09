"""Test finalise functionality for youtube videos."""
import pandas as pd
import pytest

from phoenix.tag import finalise


@pytest.fixture
def youtube_videos_for_join():
    """Tweets for the join."""
    return pd.DataFrame(
        {
            "id": ["1", "2", "3", "4"],
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
            "text": ["text_1", "text_2", "text_3", "text_4"],
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


def test_for_objects_youtube_videos(
    youtube_videos_for_join, objects_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to youtube_videos."""
    result_df = finalise.for_object_type(
        "youtube_videos",
        df=youtube_videos_for_join,
        objects_df=objects_for_join,
        language_sentiment_objects_df=language_sentiment_objects_for_join,
    )
    expected_df = pd.DataFrame(
        {
            "id": ["1", "2", "3", "4"],
            "object_type": ["ot", "ot", "ot", "ot"],
            "url": ["url1", "url2", "url3", "url4"],
            "year_filter": [2021, 2021, 2021, 2021],
            "month_filter": [1, 1, 2, 2],
            "topic": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
            "has_topic": [True, True, False, True],
            "text": ["text_1", "text_2", "text_3", "text_4"],
            "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
            "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
        },
        index=pd.Index(["1", "2", "3", "4"], name="object_id"),
    )
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_join_object_to_youtube_videos_none_objects_to_join(
    youtube_videos_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to facebook posts."""
    result_df = finalise.for_object_type(
        "youtube_videos",
        df=youtube_videos_for_join,
        language_sentiment_objects_df=language_sentiment_objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
                "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
                "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )


def test_join_object_to_youtube_videos_none_langauge_sentiment_objects(
    youtube_videos_for_join, objects_for_join
):
    """Test the join of objects to youtube_videos."""
    result_df = finalise.for_object_type(
        "youtube_videos",
        df=youtube_videos_for_join,
        objects_df=objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
                "topic": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_topic": [True, True, False, True],
                "text": ["text_1", "text_2", "text_3", "text_4"],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )


def test_join_object_to_youtube_videos_none(youtube_videos_for_join):
    """Test the join of objects to youtube_videos."""
    result_df = finalise.for_object_type(
        "youtube_videos",
        df=youtube_videos_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "id": ["1", "2", "3", "4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
            },
            index=pd.Index(["1", "2", "3", "4"], name="object_id"),
        ),
    )
