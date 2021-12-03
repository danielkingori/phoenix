"""Test finalise functionality for inherit facebook comments."""
import pandas as pd
import pytest

from phoenix.tag import finalise


@pytest.fixture
def input_comments_df():
    return pd.DataFrame(
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


@pytest.fixture
def input_facebook_posts_topics_df():
    return pd.DataFrame(
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


def test_inherit_facebook_comment_topics_from_posts(
    input_comments_df, input_facebook_posts_topics_df
):
    # Default does not inherit the `topic` or `has_topic` columns
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "post_id": [123, 456],
            "topics": ["['a']", "['a', 'b']"],
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

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_inherit_facebook_comment_topics_from_posts_rename(
    input_comments_df, input_facebook_posts_topics_df
):
    # Default does not inherit the `topic` or `has_topic` columns
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2"],
            "post_id": [123, 456],
            "classes": ["['a']", "['a', 'b']"],
            "has_classes": [True, True],
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
        input_facebook_posts_topics_df, input_comments_df, rename_topic_to_class=True
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_inherit_facebook_comment_topics_from_posts_inherit_every_row(
    input_facebook_posts_topics_df, input_comments_df
):
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2"],
            "post_id": [123, 456, 456],
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
            "comments_only_column": ["some_str", "another_str", "another_str"],
        }
    )

    output_df = finalise.inherit_facebook_comment_topics_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df, input_comments_df, inherit_every_row_per_id=True
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_inherit_facebook_comment_topics_from_posts_inherit_every_row_rename(
    input_facebook_posts_topics_df, input_comments_df
):
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2"],
            "post_id": [123, 456, 456],
            "classes": ["['a']", "['a', 'b']", "['a', 'b']"],
            "has_classes": [True, True, True],
            "is_economic_labour_tension": [True, True, True],
            "is_political_tension": [True, True, True],
            "is_service_related_tension": [True, True, True],
            "is_community_insecurity_tension": [True, True, True],
            "is_sectarian_tension": [True, True, True],
            "is_environmental_tension": [True, True, True],
            "is_geopolitics_tension": [True, True, True],
            "is_intercommunity_relations_tension": [True, True, True],
            "has_tension": [True, True, True],
            "comments_only_column": ["some_str", "another_str", "another_str"],
        }
    )

    output_df = finalise.inherit_facebook_comment_topics_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df,
        input_comments_df,
        inherit_every_row_per_id=True,
        rename_topic_to_class=True,
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_inherit_facebook_comment_topics_from_posts_inherit_every_row_extra_inherited_col(
    input_facebook_posts_topics_df, input_comments_df
):
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2"],
            "post_id": [123, 456, 456],
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
            "comments_only_column": ["some_str", "another_str", "another_str"],
        }
    )

    output_df = finalise.inherit_facebook_comment_topics_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df,
        input_comments_df,
        inherit_every_row_per_id=True,
        extra_inherited_cols=["topic", "has_topic"],
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_inherit_facebook_comment_topics_from_posts_inherit_every_row_extra_inherited_col_rename(
    input_facebook_posts_topics_df, input_comments_df
):
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2"],
            "post_id": [123, 456, 456],
            "class": ["a", "a", "b"],
            "has_class": [True, True, True],
            "classes": ["['a']", "['a', 'b']", "['a', 'b']"],
            "has_classes": [True, True, True],
            "is_economic_labour_tension": [True, True, True],
            "is_political_tension": [True, True, True],
            "is_service_related_tension": [True, True, True],
            "is_community_insecurity_tension": [True, True, True],
            "is_sectarian_tension": [True, True, True],
            "is_environmental_tension": [True, True, True],
            "is_geopolitics_tension": [True, True, True],
            "is_intercommunity_relations_tension": [True, True, True],
            "has_tension": [True, True, True],
            "comments_only_column": ["some_str", "another_str", "another_str"],
        }
    )

    output_df = finalise.inherit_facebook_comment_topics_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df,
        input_comments_df,
        inherit_every_row_per_id=True,
        extra_inherited_cols=["topic", "has_topic"],
        rename_topic_to_class=True,
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)
