"""Test finalise functionality."""
import pandas as pd
import pytest

from phoenix.tag import finalise


@pytest.fixture
def facebook_posts_to_join():
    return pd.DataFrame(
        {
            "phoenix_post_id": ["o1", "o2"],
            "object_type": ["ot", "ot"],
            "url": ["url1", "url2"],
        }
    )


@pytest.fixture
def topics_facebook_posts_to_join():
    return pd.DataFrame(
        {
            "object_id": ["o1", "o1", "o2", "o3"],
            "topic": ["o1", "o1", "o2", "o3"],
            "matched_features": ["mf", "mf", "mf", "mf"],
            "has_topic": [True, True, True, True],
        }
    )


def test_join_topics_to_facebook_posts(topics_facebook_posts_to_join, facebook_posts_to_join):
    """Test the join of topics to facebook posts."""
    result_df = finalise.join_topics_to_facebook_posts(
        topics_facebook_posts_to_join, facebook_posts_to_join
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_topic": [True, True, True],
                "phoenix_post_id": ["o1", "o1", "o2"],
                "object_type": ["ot", "ot", "ot"],
                "url": ["url1", "url1", "url2"],
            }
        ),
    )


def test_join_topics_to_facebook_posts_rename(
    topics_facebook_posts_to_join, facebook_posts_to_join
):
    """Test the join of topics to facebook posts with rename_topic_to_class."""
    result_df = finalise.join_topics_to_facebook_posts(
        topics_facebook_posts_to_join, facebook_posts_to_join, rename_topic_to_class=True
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2"],
                "class": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_class": [True, True, True],
                "phoenix_post_id": ["o1", "o1", "o2"],
                "object_type": ["ot", "ot", "ot"],
                "url": ["url1", "url1", "url2"],
            }
        ),
    )


@pytest.fixture
def topics_to_join():
    return pd.DataFrame(
        {
            "object_id": ["1", "1", "2", "3"],
            "topic": ["o1", "o1", "o2", "o2"],
            "matched_features": ["mf", "mf", "mf", "mf"],
            "has_topic": [True, True, True, True],
        }
    )


@pytest.fixture
def tweets_to_join():
    return pd.DataFrame(
        {
            "id_str": [1, 2],
            "url": ["url1", "url2"],
            "retweeted": [True, False],
            "object_type": ["ot", "ot"],
        }
    )


def test_join_topics_to_tweets(topics_to_join, tweets_to_join):
    """Test the join of topics to tweets."""
    result_df = finalise.join_topics_to_tweets(topics_to_join, tweets_to_join)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_topic": [True, True, True],
                "id_str": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


def test_join_topics_to_tweets_rename(topics_to_join, tweets_to_join):
    """Test the join of topics to tweets with rename_topic_to_class."""
    result_df = finalise.join_topics_to_tweets(
        topics_to_join, tweets_to_join, rename_topic_to_class=True
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "class": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_class": [True, True, True],
                "id_str": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


@pytest.fixture
def facebook_comments_to_join():
    return pd.DataFrame(
        {
            "id": [1, 2],
            "url": ["url1", "url2"],
            "bool_prop": [True, False],
            "object_type": ["ot", "ot"],
        }
    )


def test_join_topics_to_facebook_comments(topics_to_join, facebook_comments_to_join):
    """Test the join of topics to facebook comments."""
    result_df = finalise.join_topics_to_facebook_comments(
        topics_to_join, facebook_comments_to_join
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_topic": [True, True, True],
                "id": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "bool_prop": [True, True, False],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


def test_join_topics_to_facebook_comments_rename(topics_to_join, facebook_comments_to_join):
    """Test the join of topics to facebook comments with rename_topic_to_class."""
    result_df = finalise.join_topics_to_facebook_comments(
        topics_to_join, facebook_comments_to_join, rename_topic_to_class=True
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "class": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_class": [True, True, True],
                "id": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "bool_prop": [True, True, False],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


@pytest.fixture
def youtube_videos_to_join():
    return pd.DataFrame(
        {
            "id": ["1", "2"],
            "url": ["url1", "url2"],
            "text": ["text", "text"],
            "object_type": ["ot", "ot"],
        }
    )


def test_topics_for_objects_type_youtube_videos(topics_to_join, youtube_videos_to_join):
    """Test the join of topics to youtube_videos."""
    result_df = finalise.topics_for_object_type(
        "youtube_videos", df=youtube_videos_to_join, topics_df=topics_to_join
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_topic": [True, True, True],
                "id": ["1", "1", "2"],
                "url": ["url1", "url1", "url2"],
                "text": ["text", "text", "text"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


def test_topics_for_objects_type_youtube_videos_rename(topics_to_join, youtube_videos_to_join):
    """Test the join of topics to youtube_videos with rename."""
    result_df = finalise.topics_for_object_type(
        "youtube_videos",
        df=youtube_videos_to_join,
        topics_df=topics_to_join,
        rename_topic_to_class=True,
    )
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "class": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "has_class": [True, True, True],
                "id": ["1", "1", "2"],
                "url": ["url1", "url1", "url2"],
                "text": ["text", "text", "text"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )
