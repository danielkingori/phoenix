"""Test Feature."""
import pandas as pd

from phoenix.tag import feature


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
