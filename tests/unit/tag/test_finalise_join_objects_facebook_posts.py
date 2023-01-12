"""Test finalise functionality."""
import pandas as pd
import pytest

from phoenix.tag import finalise


@pytest.fixture
def facebook_posts_for_join():
    """Facebook posts for the join."""
    return pd.DataFrame(
        {
            "phoenix_post_id": ["o1", "o2", "o3", "o4"],
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
            "object_id": ["o1", "o2", "o3", "o4"],
            "topics": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
            "has_topics": [True, True, False, True],
        }
    )


@pytest.fixture
def language_sentiment_objects_for_join():
    """Language Sentiment Objects for the join."""
    return pd.DataFrame(
        {
            "object_id": ["o1", "o2", "o3", "o4"],
            "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
            "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
            "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
        }
    )


def test_join_object_to_facebook_posts(
    facebook_posts_for_join, objects_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_facebook_posts(
        facebook_posts_df=facebook_posts_for_join,
        objects_df=objects_for_join,
        language_sentiment_objects_df=language_sentiment_objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "phoenix_post_id": ["o1", "o2", "o3", "o4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
                "topics": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_topics": [True, True, False, True],
                "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
                "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
            },
            index=pd.Index(["o1", "o2", "o3", "o4"], name="object_id"),
        ),
    )


def test_join_object_to_facebook_posts_rename(
    facebook_posts_for_join, objects_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_facebook_posts(
        facebook_posts_df=facebook_posts_for_join,
        objects_df=objects_for_join,
        language_sentiment_objects_df=language_sentiment_objects_for_join,
        rename_topic_to_class=True,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "phoenix_post_id": ["o1", "o2", "o3", "o4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
                "topics": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_topics": [True, True, False, True],
                "classes": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_classes": [True, True, False, True],
                "language_sentiment": ["POSITIVE", "NEGATIVE", "POSITIVE", "POSITIVE"],
                "language_sentiment_score_mixed": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_neutral": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_negative": [0.9, 0.9, 0.8, 0.8],
                "language_sentiment_score_positive": [0.9, 0.9, 0.8, 0.8],
            },
            index=pd.Index(["o1", "o2", "o3", "o4"], name="object_id"),
        ),
    )


def test_join_object_to_facebook_posts_none_objects(
    facebook_posts_for_join, language_sentiment_objects_for_join
):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_facebook_posts(
        facebook_posts_df=facebook_posts_for_join,
        language_sentiment_objects_df=language_sentiment_objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "phoenix_post_id": ["o1", "o2", "o3", "o4"],
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
            index=pd.Index(["o1", "o2", "o3", "o4"], name="object_id"),
        ),
    )


def test_join_object_to_facebook_posts_none_langauge_sentiment_objects(
    facebook_posts_for_join, objects_for_join
):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_facebook_posts(
        facebook_posts_df=facebook_posts_for_join,
        objects_df=objects_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "phoenix_post_id": ["o1", "o2", "o3", "o4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
                "topics": [["t_1", "t_2"], ["t_2"], [], ["t_4"]],
                "has_topics": [True, True, False, True],
            },
            index=pd.Index(["o1", "o2", "o3", "o4"], name="object_id"),
        ),
    )


def test_join_object_to_facebook_posts_none(facebook_posts_for_join):
    """Test the join of objects to facebook posts."""
    result_df = finalise.join_objects_to_facebook_posts(
        facebook_posts_df=facebook_posts_for_join,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "phoenix_post_id": ["o1", "o2", "o3", "o4"],
                "object_type": ["ot", "ot", "ot", "ot"],
                "url": ["url1", "url2", "url3", "url4"],
                "year_filter": [2021, 2021, 2021, 2021],
                "month_filter": [1, 1, 2, 2],
            },
            index=pd.Index(["o1", "o2", "o3", "o4"], name="object_id"),
        ),
    )
