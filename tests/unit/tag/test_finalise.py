"""Test finalise functionality."""
import pandas as pd

from phoenix.tag import finalise


def test_join_topics_to_facebook_posts():
    """Test the join of topics to facebook posts."""
    topics = pd.DataFrame(
        {
            "object_id": ["o1", "o1", "o2", "o3"],
            "topics": ["o1", "o1", "o2", "o3"],
        }
    )

    facebook_posts = pd.DataFrame(
        {
            "phoenix_post_id": ["o1", "o2"],
            "url": ["url1", "url2"],
        }
    )

    result_df = finalise.join_topics_to_facebook_posts(topics, facebook_posts)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2"],
                "topics": ["o1", "o1", "o2"],
                "phoenix_post_id": ["o1", "o1", "o2"],
                "url": ["url1", "url1", "url2"],
            }
        ),
    )


def test_join_topics_to_tweets():
    """Test the join of topics to tweets."""
    topics = pd.DataFrame(
        {
            "object_id": ["1", "1", "2", "3"],
            "topics": ["o1", "o1", "o2", "o2"],
        }
    )

    tweets = pd.DataFrame(
        {
            "id_str": [1, 2],
            "url": ["url1", "url2"],
            "retweeted": [True, False],
        }
    )

    result_df = finalise.join_topics_to_tweets(topics, tweets)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["1", "1", "2"],
                "topics": ["o1", "o1", "o2"],
                "id_str": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
            }
        ),
    )
