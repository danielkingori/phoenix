"""Test Feature Object processing."""
import mock
import pandas as pd
import pytest

from phoenix.tag import feature_object_process


@pytest.fixture
def exploded_features_df():
    """Get exploded_features_df."""
    return pd.DataFrame(
        {
            "object_id": [1, 1, 2, 2, 3, 3],
            "features": ["1-f1", "1-f2", "2-f1", "2-f2", "3-f1", "3-f2"],
            "features_count": [2, 1, 1, 2, 3, 3],
            "non_feature_column": [None, True, False, None, None, None],
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


@pytest.fixture
def is_key_object_s():
    """Get the is key object s."""
    return pd.Series(
        [True, True, False, False, True, True],
        name="features",
        index=pd.Int64Index([1, 1, 2, 2, 3, 3], dtype="int64", name="object_id"),
    )


@pytest.fixture
def exploded_features_with_is_key_object(exploded_features_df, is_key_object_s):
    """Get the is key feature s."""
    df = exploded_features_df.copy()
    df["is_key_object"] = is_key_object_s
    return df


def test_group_key_objects(exploded_features_df, is_key_feature_s):
    """Test group key objects."""
    exploded_features_df["is_key_feature"] = is_key_feature_s
    output = feature_object_process.group_key_objects(exploded_features_df)
    pd.testing.assert_frame_equal(
        output,
        pd.DataFrame(
            {
                "object_id": [1, 3],
                "non_feature_column": [True, None],
                "has_key_feature": [True, True],
            },
            index=pd.Index([1, 3], name="object_id"),
        ),
    )


@mock.patch("phoenix.tag.object_filters.get_all_key_objects")
@mock.patch("phoenix.tag.feature_object_process.group_key_objects")
def test_get_key_objects(m_group_key_objects, m_get_all_key_objects):
    """Test group key objects."""
    m_df = mock.MagicMock(pd.DataFrame)
    features_key = "features"
    output = feature_object_process.get_key_objects(m_df, features_key)
    m_group_key_objects.assert_called_once_with(m_df, features_key)
    m_get_all_key_objects.assert_called_once_with(m_group_key_objects.return_value)
    m_get_all_key_objects.return_value.assert_has_calls(
        [mock.call.__setitem__("is_key_object", True)]
    )
    assert output == m_get_all_key_objects.return_value


def test_features_with_is_key_feature(exploded_features_df, exploded_features_with_is_key_object):
    """Test features_with_is_key_feature."""
    key_objects = pd.DataFrame(
        {"object_id": [1, 3], "is_key_object": [True, True]},
        index=pd.Index([1, 3], name="object_id"),
    )
    result = feature_object_process.features_with_is_key_feature(exploded_features_df, key_objects)
    pd.testing.assert_frame_equal(result, exploded_features_with_is_key_object)


def test_join_all_obects(exploded_features_df, is_key_object_s):
    """Test finalise."""
    all_objects_df = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["1-text", "2-text", "3-text"],
        }
    )
    object_features = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "features": [["1-f1", "1-f2"], ["2-f1", "2-f2"], ["3-f1", "3-f2"]],
            "features_count": [[2, 1], [1, 2], [3, 3]],
        },
        index=pd.Index([1, 2, 3], name="object_id"),
    )
    key_objects = pd.DataFrame(
        {"object_id": [1, 3], "is_key_object": [True, True]},
        index=pd.Index([1, 3], name="object_id"),
    )
    objects = feature_object_process.join_all_objects(all_objects_df, key_objects, object_features)
    e_objects = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["1-text", "2-text", "3-text"],
            "is_key_object": [True, False, True],
            "features": [["1-f1", "1-f2"], ["2-f1", "2-f2"], ["3-f1", "3-f2"]],
            "features_count": [[2, 1], [1, 2], [3, 3]],
        },
    )
    pd.testing.assert_frame_equal(objects, e_objects)


@mock.patch("phoenix.tag.object_filters.get_key_objects")
@mock.patch("phoenix.tag.feature_object_process.join_all_objects")
@mock.patch("phoenix.tag.feature_object_process.get_feature_objects")
@mock.patch("phoenix.tag.feature_object_process.features_with_is_key_feature")
@mock.patch("phoenix.tag.feature_object_process.get_key_objects")
@mock.patch("phoenix.tag.feature.key_features")
def test_finalise(
    m_key_features,
    m_get_key_objects,
    m_features_with_is_key_feature,
    m_get_feature_objects,
    m_join_all_objects,
    m_filter_get_key_objects,
):
    """Test finalise calls correct functions."""
    all_objects_df = mock.MagicMock(pd.DataFrame)
    exploded_features_df = mock.MagicMock(pd.DataFrame)
    features_key = "key"
    objects, key_objects, features = feature_object_process.finalise(
        all_objects_df, exploded_features_df, features_key
    )
    exploded_features_df.copy.assert_called_once()
    df_copy = exploded_features_df.copy.return_value
    df_copy_get = df_copy.__getitem__.return_value
    m_key_features.assert_called_once_with(df_copy_get)
    m_get_key_objects.assert_called_once_with(df_copy)
    m_features_with_is_key_feature.assert_called_once_with(df_copy, m_get_key_objects.return_value)
    m_get_feature_objects.assert_called_once_with(m_features_with_is_key_feature.return_value)
    m_join_all_objects.assert_called_once_with(
        all_objects_df, m_get_key_objects.return_value, m_get_feature_objects.return_value
    )
    m_filter_get_key_objects.assert_called_once_with(m_join_all_objects.return_value)
    assert objects == m_join_all_objects.return_value
    assert key_objects == m_filter_get_key_objects.return_value
    assert features == m_features_with_is_key_feature.return_value
