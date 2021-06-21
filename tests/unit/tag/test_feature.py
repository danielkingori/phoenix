"""Test Feature."""
import mock
import pandas as pd

from phoenix.tag import feature


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
