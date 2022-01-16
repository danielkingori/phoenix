"""Test finalise functionality for inherit facebook comments."""
import pandas as pd
import pytest

from phoenix.tag import finalise_facebook_comments


def input_facebook_posts_topics_df():
    row_count = 4
    return pd.DataFrame(
        {
            "id": ["1", "2", "2", "3"],
            "url_post_id": ["123", "456", "456", "789"],
            "topic": ["a", "a", "b", "non_topic"],
            "has_topic": [True, True, True, False],
            "features": ["a", "a", "b", "non_topic"],
            "features_count": [1, 2, 3, 4],
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
            "non_default_inherited": [True] * row_count,
        }
    )


@pytest.mark.parametrize(
    "posts_topics_df, expected_result",
    [
        (
            input_facebook_posts_topics_df(),
            finalise_facebook_comments.FACEBOOK_COMMENT_INHERITABLE_COLUMNS,
        ),
        (
            input_facebook_posts_topics_df().drop(columns=["has_tension"]),
            finalise_facebook_comments.FACEBOOK_COMMENT_INHERITABLE_COLUMNS[:-1],
        ),
    ],
)
def test_inherited_columns_for_facebook_comments(posts_topics_df, expected_result):
    """Test inherited_columns_for_facebook_comments."""
    result = finalise_facebook_comments.inherited_columns_for_facebook_comments(
        posts_topics_df=posts_topics_df,
    )
    assert result.sort() == expected_result.sort()


@pytest.mark.parametrize(
    "posts_topics_df, expected_result",
    [
        (
            input_facebook_posts_topics_df(),
            finalise_facebook_comments.FACEBOOK_COMMENT_TOPICS_INHERITABLE_COLUMNS,
        ),
        (
            input_facebook_posts_topics_df().drop(columns=["has_tension"]),
            finalise_facebook_comments.FACEBOOK_COMMENT_TOPICS_INHERITABLE_COLUMNS[:-1],
        ),
    ],
)
def test_inherited_columns_for_facebook_comments_topics(posts_topics_df, expected_result):
    """Test inherited_columns_for_facebook_comments_topics."""
    result = finalise_facebook_comments.inherited_columns_for_facebook_comments_topics(
        posts_topics_df=posts_topics_df,
    )
    assert result.sort() == expected_result.sort()
