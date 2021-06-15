"""Test Feature."""
import mock
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


@mock.patch("phoenix.tag.feature.get_interesting_features")
def test_key_features(m_get_interesting_features):
    """Test get key features."""
    m_get_interesting_features.return_value = pd.DataFrame(
        {"interesting_features": ["match", "i_not_match"]}
    )
    input_df = pd.DataFrame({"features": ["match", "not_match"]})
    output_s = feature.key_features(input_df)
    pd.testing.assert_series_equal(output_s, pd.Series([True, False], name="features"))
