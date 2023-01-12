"""Test Feature Object processing."""
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


def test_finalise(exploded_features_df):
    """Test finalise."""
    all_objects_df = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["1-text", "2-text", "3-text"],
        }
    )
    objects = feature_object_process.finalise(all_objects_df, exploded_features_df)
    e_objects = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["1-text", "2-text", "3-text"],
            "features": [["1-f1", "1-f2"], ["2-f1", "2-f2"], ["3-f1", "3-f2"]],
            "features_count": [[2, 1], [1, 2], [3, 3]],
        },
    )
    pd.testing.assert_frame_equal(objects, e_objects)
