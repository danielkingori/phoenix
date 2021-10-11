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
            "has_topic": [True, True, True, True],
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
                "has_topic": [True, True, True],
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
            "has_topic": [True, True, True, True],
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
                "has_topic": [True, True, True],
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
            "has_topic": [True, True, True, True],
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
                "has_topic": [True, True, True],
                "id": [1, 1, 2],
                "url": ["url1", "url1", "url2"],
                "object_type": ["ot", "ot", "ot"],
            }
        ),
    )


def inherit_facebook_comment_topics_from_posts():
    input_comments_df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "post_id": [123, 456],
            "topics": ["[None]", "[None]"],
            "has_topics": [False, False],
            "is_economic_labour_tension": [False, False],
            "is_political_tension": [False, False],
            "is_service_related_tension": [False, False],
            "is_community_insecurity_tension": [False, False],
            "is_sectarian_tension": [False, False],
            "is_environmental_tension": [False, False],
            "is_geopolitics_tension": [False, False],
            "is_intercommunity_relations_tension": [False, False],
            "has_tension": [False, False],
            "comments_only_column": ["some_str", "another_str"],
        }
    )

    input_facebook_posts_topics_df = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "url_post_id": ["123", "456", "456"],
            "topic": ["a", "a", "b"],
            "has_topic": [True, True, True],
            "topics": ["['a']", "['a', 'b']", "['a', 'b']"],
            "has_topics": [True, True, True],
            "is_economic_labour_tension": [True, True, True],
            "is_political_tension": [True, True, True],
            "is_service_related_tension": [True, True, True],
            "is_community_insecurity_tension": [True, True, True],
            "is_sectarian_tension": [True, True, True],
            "is_environmental_tension": [True, True, True],
            "is_geopolitics_tension": [True, True, True],
            "is_intercommunity_relations_tension": [True, True, True],
            "has_tension": [True, True, True],
        }
    )

    # Default does not inherit the `topic` or `has_topic` columns
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "post_id": [123, 456],
            "topics": ["a", "a"],
            "has_topics": [True, True],
            "is_economic_labour_tension": [True, True],
            "is_political_tension": [True, True],
            "is_service_related_tension": [True, True],
            "is_community_insecurity_tension": [True, True],
            "is_sectarian_tension": [True, True],
            "is_environmental_tension": [True, True],
            "is_geopolitics_tension": [True, True],
            "is_intercommunity_relations_tension": [True, True],
            "has_tension": [True, True],
            "comments_only_column": ["some_str", "another_str"],
        }
    )

    output_df = finalise.inherit_facebook_comment_topics_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df, input_comments_df
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df)
