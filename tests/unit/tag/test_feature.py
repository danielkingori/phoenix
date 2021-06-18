"""Test Feature."""
import mock
import pandas as pd
import pytest

from phoenix.tag import feature


@pytest.fixture
def exploded_features_df():
    """Get exploded_features_df."""
    return pd.DataFrame(
        {
            "object_id": [1, 1, 2, 2, 3, 3],
            "features": ["1-f1", "1-f2", "2-f1", "2-f2", "3-f1", "3-f2"],
            "features_count": [2, 1, 1, 2, 3, 3],
        },
        index=pd.Index([1, 1, 2, 2, 3, 3], name="object_id"),
    )


@pytest.fixture
def is_key_feature_s():
    """Get the is key feature s."""
    return pd.Series(
        [False, True, False, False, True, False],
        name="features",
        index=pd.Int64Index([1, 1, 2, 2, 3, 3], dtype="int64", name="object_id"),
    )


def test_explode_features():
    """Test explode features."""
    input_df = pd.DataFrame(
        {
            "object_id": [1, 2],
            "features": [
                ["1-f1", "1-f1", "1-f2"],
                ["2-f1", "2-f2", "2-f2"],
            ],
        }
    )

    output_df = feature.explode_features(input_df)
    pd.testing.assert_frame_equal(
        output_df,
        pd.DataFrame(
            {
                "object_id": [1, 1, 2, 2],
                "features": ["1-f1", "1-f2", "2-f1", "2-f2"],
                "features_count": [2, 1, 1, 2],
            },
            index=pd.Index([1, 1, 2, 2], name="object_id"),
        ),
    )


@mock.patch("phoenix.tag.feature.get_interesting_features")
def test_key_features(m_get_interesting_features):
    """Test get key features."""
    m_get_interesting_features.return_value = pd.DataFrame(
        {"interesting_features": ["match", "i_not_match"]}
    )
    input_df = pd.DataFrame(
        {"features": ["match", "not_match", "not_match", "match", "not_match"]},
        index=pd.Index([1, 1, 1, 2, 2], name="object_id"),
    )
    output_s = feature.key_features(input_df)
    pd.testing.assert_series_equal(
        output_s,
        pd.Series(
            [True, False, False, True, False],
            name="features",
            index=pd.Int64Index([1, 1, 1, 2, 2], dtype="int64", name="object_id"),
        ),
    )


def test_get_key_items(exploded_features_df, is_key_feature_s):
    """Test get key items."""
    exploded_features_df["is_key_feature"] = is_key_feature_s
    output = feature.get_key_items(exploded_features_df)
    pd.testing.assert_frame_equal(
        output,
        pd.DataFrame(
            {
                "object_id": [1, 3],
                "has_key_feature": [True, True],
            },
            index=pd.Index([1, 3], name="object_id"),
        ),
    )


@mock.patch("phoenix.tag.feature.key_features")
def test_finalise(m_key_features, exploded_features_df, is_key_feature_s):
    """Test finalise."""
    m_key_features.return_value = is_key_feature_s
    all_items_df = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["1-text", "2-text", "3-text"],
        }
    )
    items, key_items, features_has = feature.finalise(all_items_df, exploded_features_df)
    m_key_features.assert_called_once()
    e_items = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["1-text", "2-text", "3-text"],
            "has_key_feature": [True, False, True],
            "features": [["1-f1", "1-f2"], ["2-f1", "2-f2"], ["3-f1", "3-f2"]],
            "features_count": [[2, 1], [1, 2], [3, 3]],
        },
    )
    pd.testing.assert_frame_equal(items, e_items)
    pd.testing.assert_frame_equal(key_items, e_items[e_items["has_key_feature"].isin([True])])
    e_features_has = exploded_features_df.copy()
    e_features_has["is_key_feature"] = is_key_feature_s
    e_features_has["has_key_feature"] = pd.Series(
        [True, True, False, False, True, True],
        index=pd.Int64Index([1, 1, 2, 2, 3, 3], dtype="int64", name="object_id"),
    )
    pd.testing.assert_frame_equal(features_has, e_features_has)
