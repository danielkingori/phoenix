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
