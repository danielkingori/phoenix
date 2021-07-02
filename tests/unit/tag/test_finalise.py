"""Test finalise functionality."""
import pandas as pd

from phoenix.tag import finalise


def test_join_topics_to_facebook_posts():
    """Test the join of topics to facebook posts."""
    topics = pd.DataFrame(
        {
            "object_id": ["o1", "o1", "o2", "o3"],
            "topic": ["o1", "o1", "o2", "o3"],
            "matched_features": ["mf", "mf", "mf", "mf"],
        }
    )

    facebook_posts = pd.DataFrame(
        {
            "phoenix_post_id": ["o1", "o2"],
            "object_type": ["ot", "ot"],
            "url": ["url1", "url2"],
        }
    )

    result_df = finalise.join_topics_to_facebook_posts(topics, facebook_posts)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "phoenix_post_id": ["o1", "o1", "o2"],
                "object_type": ["ot", "ot", "ot"],
                "url": ["url1", "url1", "url2"],
            }
        ),
    )


def test_join_topics_to_tweets():
    """Test the join of topics to tweets."""
    topics = pd.DataFrame(
        {
            "object_id": ["1", "1", "2", "3"],
            "topic": ["o1", "o1", "o2", "o2"],
            "matched_features": ["mf", "mf", "mf", "mf"],
        }
    )

    tweets = pd.DataFrame(
        {
            "id_str": [1, 2],
            "url": ["url1", "url2"],
            "retweeted": [True, False],
            "object_type": ["ot", "ot"],
        }
    )

    result_df = finalise.join_topics_to_tweets(topics, tweets)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "id_str": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


def test_join_topics_to_facebook_comments():
    """Test the join of topics to facebook comments."""
    topics = pd.DataFrame(
        {
            "object_id": ["1", "1", "2", "3"],
            "topic": ["o1", "o1", "o2", "o2"],
            "matched_features": ["mf", "mf", "mf", "mf"],
        }
    )

    comments = pd.DataFrame(
        {
            "id": [1, 2],
            "url": ["url1", "url2"],
            "object_type": ["ot", "ot"],
        }
    )

    result_df = finalise.join_topics_to_facebook_comments(topics, comments)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "topic": ["o1", "o1", "o2"],
                "matched_features": ["mf", "mf", "mf"],
                "id": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )
