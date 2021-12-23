"""Test Feature."""
import mock
import pandas as pd

from phoenix.tag import feature


@mock.patch("phoenix.tag.feature.text_features_analyser.create")
def test_features(mock_create):
    """Test that two tfa's are created, one parallelisable and one not."""
    input_df = pd.DataFrame(
        data={
            "clean_text": [
                "hello",
                "Min nizanibû a ku bin min",
                "لە ســـاڵەکانی ١٩٥٠دا یان",
                " مبادرة #البابا_فرنسيس في يوم #لبنان ",
                ":muscle:",
            ],
            "language": ["en", "ku", "ckb", "ar", "und"],
            "another_column": ["foo", "bar", "baz", "spam", "ham"],
        }
    )

    expected_df = pd.DataFrame(
        data={
            "clean_text": [
                "hello",
                "Min nizanibû a ku bin min",
                "لە ســـاڵەکانی ١٩٥٠دا یان",
                " مبادرة #البابا_فرنسيس في يوم #لبنان ",
                ":muscle:",
            ],
            "language": ["en", "ku", "ckb", "ar", "und"],
            "another_column": ["foo", "bar", "baz", "spam", "ham"],
            "features": ["a", "b", "c", "d", "e"],
        }
    )

    mock_parallelisable_tfa = mock.MagicMock()
    mock_non_parallelisable_tfa = mock.MagicMock()
    mock_create.side_effect = [mock_parallelisable_tfa, mock_non_parallelisable_tfa]

    mock_parallelisable_tfa.features.return_value = pd.Series(
        index=[0, 3, 4], data=["a", "d", "e"]
    )
    mock_non_parallelisable_tfa.features.return_value = pd.Series(index=[1, 2], data=["b", "c"])

    actual_df = feature.features(input_df)

    mock_create.assert_has_calls(
        [
            mock.call(parallelisable=True),
            mock.call(parallelisable=False),
        ]
    )

    pd.testing.assert_frame_equal(expected_df, actual_df)
