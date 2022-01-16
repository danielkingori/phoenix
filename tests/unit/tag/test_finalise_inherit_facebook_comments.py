"""Test finalise functionality for inherit facebook comments."""
import pandas as pd
import pytest

from phoenix.tag import finalise, finalise_facebook_comments


@pytest.fixture
def input_comments_df():
    row_count = 3
    return pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "post_id": [123, 456, 789],
            "topics": [[], [], ["c_t"]],
            "has_topics": [False, False, True],
            "is_economic_labour_tension": [False] * row_count,
            "is_political_tension": [False] * row_count,
            "is_service_related_tension": [False] * row_count,
            "is_community_insecurity_tension": [False] * row_count,
            "is_sectarian_tension": [False] * row_count,
            "is_environmental_tension": [False] * row_count,
            "is_geopolitics_tension": [False] * row_count,
            "is_intercommunity_relations_tension": [False] * row_count,
            "has_tension": [False] * row_count,
            "comments_only_column": ["some_str"] * row_count,
        }
    )


@pytest.fixture
def input_facebook_posts_topics_df():
    row_count = 4
    return pd.DataFrame(
        {
            "id": ["1", "2", "2", "3"],
            "url_post_id": ["123", "456", "456", "789"],
            "topic": ["a", "a", "b", "non_topic"],
            "has_topic": [True, True, True, False],
            "topics": [["a"], ["a", "b"], ["a", "b"], ["non_topic"]],
            "has_topics": [True, True, True, False],
            "is_economic_labour_tension": [True] * row_count,
            "is_political_tension": [True] * row_count,
            "is_service_related_tension": [True] * row_count,
            "is_community_insecurity_tension": [True] * row_count,
            "is_sectarian_tension": [True] * row_count,
            "is_environmental_tension": [True] * row_count,
            "is_geopolitics_tension": [True] * row_count,
            "is_intercommunity_relations_tension": [True] * row_count,
            "has_tension": [True] * row_count,
        }
    )


@pytest.fixture
def input_facebook_posts_classes_df(input_facebook_posts_topics_df):
    return finalise.topic_to_class(input_facebook_posts_topics_df)


def test_facebook_comments_inherit_from_facebook_posts_topics(
    input_comments_df, input_facebook_posts_topics_df
):
    # Default does not inherit the `topic` or `has_topic` columns
    row_count = 3
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "post_id": [123, 456, 789],
            "topics": [["a"], ["a", "b"], ["non_topic"]],
            "has_topics": [True, True, False],
            "is_economic_labour_tension": [True] * row_count,
            "is_political_tension": [True] * row_count,
            "is_service_related_tension": [True] * row_count,
            "is_community_insecurity_tension": [True] * row_count,
            "is_sectarian_tension": [True] * row_count,
            "is_environmental_tension": [True] * row_count,
            "is_geopolitics_tension": [True] * row_count,
            "is_intercommunity_relations_tension": [True] * row_count,
            "has_tension": [True] * row_count,
            "comments_only_column": ["some_str"] * row_count,
        }
    )

    inherited_columns = finalise_facebook_comments.inherited_columns_for_facebook_comments(
        posts_topics_df=input_facebook_posts_topics_df,
    )

    output_df = finalise_facebook_comments.inherit_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df, input_comments_df, inherited_columns
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_facebook_comments_inherit_from_facebook_posts_topics_rename(
    input_comments_df, input_facebook_posts_classes_df
):
    # Default does not inherit the `topic` or `has_topic` columns
    row_count = 3
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "post_id": [123, 456, 789],
            "classes": [["a"], ["a", "b"], ["non_topic"]],
            "has_classes": [True, True, False],
            "is_economic_labour_tension": [True] * row_count,
            "is_political_tension": [True] * row_count,
            "is_service_related_tension": [True] * row_count,
            "is_community_insecurity_tension": [True] * row_count,
            "is_sectarian_tension": [True] * row_count,
            "is_environmental_tension": [True] * row_count,
            "is_geopolitics_tension": [True] * row_count,
            "is_intercommunity_relations_tension": [True] * row_count,
            "has_tension": [True] * row_count,
            "comments_only_column": ["some_str"] * row_count,
        }
    )

    inherited_columns = finalise_facebook_comments.inherited_columns_for_facebook_comments(
        posts_topics_df=input_facebook_posts_classes_df,
    )

    output_df = finalise_facebook_comments.inherit_from_facebook_posts_topics_df(
        input_facebook_posts_classes_df,
        input_comments_df,
        inherited_columns,
        rename_topic_to_class=True,
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_facebook_comments_inherit_from_facebook_posts_topics_dropped(
    input_facebook_posts_topics_df, input_comments_df
):
    row_count = 4
    to_drop = [
        "is_economic_labour_tension",
        "is_political_tension",
        "is_service_related_tension",
        "is_community_insecurity_tension",
        "is_sectarian_tension",
        "is_environmental_tension",
        "is_geopolitics_tension",
        "is_intercommunity_relations_tension",
        "has_tension",
    ]
    input_comments_df = input_comments_df.drop(columns=to_drop, axis=1)
    input_facebook_posts_topics_df = input_facebook_posts_topics_df.drop(columns=to_drop, axis=1)
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2", "3"],
            "post_id": [123, 456, 456, 789],
            "topics": [["a"], ["a", "b"], ["a", "b"], ["non_topic"]],
            "has_topics": [True, True, True, False],
            "comments_only_column": ["some_str"] * row_count,
        }
    )

    inherited_columns = finalise_facebook_comments.inherited_columns_for_facebook_comments(
        posts_topics_df=input_facebook_posts_topics_df,
    )

    output_df = finalise_facebook_comments.inherit_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df,
        input_comments_df,
        inherited_columns,
        inherit_every_row_per_id=True,
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_facebook_comments_topics_inherit_from_facebook_posts_topics(
    input_facebook_posts_topics_df, input_comments_df
):
    row_count = 4
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2", "3"],
            "post_id": [123, 456, 456, 789],
            "topic": ["a", "a", "b", "non_topic"],
            "has_topic": [True, True, True, False],
            "topics": [["a"], ["a", "b"], ["a", "b"], ["non_topic"]],
            "has_topics": [True, True, True, False],
            "is_economic_labour_tension": [True] * row_count,
            "is_political_tension": [True] * row_count,
            "is_service_related_tension": [True] * row_count,
            "is_community_insecurity_tension": [True] * row_count,
            "is_sectarian_tension": [True] * row_count,
            "is_environmental_tension": [True] * row_count,
            "is_geopolitics_tension": [True] * row_count,
            "is_intercommunity_relations_tension": [True] * row_count,
            "has_tension": [True] * row_count,
            "comments_only_column": ["some_str"] * row_count,
        }
    )

    inherited_columns = finalise_facebook_comments.inherited_columns_for_facebook_comments_topics(
        posts_topics_df=input_facebook_posts_topics_df,
    )

    output_df = finalise_facebook_comments.inherit_from_facebook_posts_topics_df(
        input_facebook_posts_topics_df,
        input_comments_df,
        inherited_columns,
        inherit_every_row_per_id=True,
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)


def test_facebook_comments_topics_inherit_from_facebook_posts_topics_rename(
    input_facebook_posts_classes_df, input_comments_df
):
    row_count = 4
    expected_comments_df = pd.DataFrame(
        {
            "id": ["1", "2", "2", "3"],
            "post_id": [123, 456, 456, 789],
            "class": ["a", "a", "b", "non_topic"],
            "has_class": [True, True, True, False],
            "classes": [["a"], ["a", "b"], ["a", "b"], ["non_topic"]],
            "has_classes": [True, True, True, False],
            "is_economic_labour_tension": [True] * row_count,
            "is_political_tension": [True] * row_count,
            "is_service_related_tension": [True] * row_count,
            "is_community_insecurity_tension": [True] * row_count,
            "is_sectarian_tension": [True] * row_count,
            "is_environmental_tension": [True] * row_count,
            "is_geopolitics_tension": [True] * row_count,
            "is_intercommunity_relations_tension": [True] * row_count,
            "has_tension": [True] * row_count,
            "comments_only_column": ["some_str"] * row_count,
        }
    )

    inherited_columns = finalise_facebook_comments.inherited_columns_for_facebook_comments_topics(
        posts_topics_df=input_facebook_posts_classes_df,
    )

    output_df = finalise_facebook_comments.inherit_from_facebook_posts_topics_df(
        input_facebook_posts_classes_df,
        input_comments_df,
        inherited_columns,
        inherit_every_row_per_id=True,
        rename_topic_to_class=True,
    )

    pd.testing.assert_frame_equal(output_df, expected_comments_df, check_like=True)
