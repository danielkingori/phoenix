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


def test_get_unprocessed_features():
    """Test the noop features returns correct ngrams."""
    input_df = pd.DataFrame(
        data={
            "clean_text": [
                "hello",
                "Min nizanibû a ku",
                "لە ســـاڵەکانی ١٩٥٠دا",
                " مبادرة #البابا_فرنسيس #لبنان ",
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
                "Min nizanibû a ku",
                "لە ســـاڵەکانی ١٩٥٠دا",
                " مبادرة #البابا_فرنسيس #لبنان ",
                ":muscle:",
            ],
            "language": ["en", "ku", "ckb", "ar", "und"],
            "another_column": ["foo", "bar", "baz", "spam", "ham"],
            "features": [
                ["hello"],
                [
                    "min",
                    "nizanibû",
                    "a",
                    "ku",
                    "min nizanibû",
                    "nizanibû a",
                    "a ku",
                    "min nizanibû a",
                    "nizanibû a ku",
                ],
                [
                    "لە",
                    "ســـاڵەکانی",
                    "١٩٥٠دا",
                    "لە ســـاڵەکانی",
                    "ســـاڵەکانی ١٩٥٠دا",
                    "لە ســـاڵەکانی ١٩٥٠دا",
                ],
                [
                    "مبادرة",
                    "#البابا_فرنسيس",
                    "#لبنان",
                    "مبادرة #البابا_فرنسيس",
                    "#البابا_فرنسيس #لبنان",
                    "مبادرة #البابا_فرنسيس #لبنان",
                ],
                ["muscle"],
            ],
        }
    )

    actual_df = feature.get_unprocessed_features(input_df)

    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_explode_features():
    """Test explode features explodes features correctly."""
    input_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_2"],
            "features": [
                ["so", "tell", "me", "what", "you", "want"],
                ["what", "you", "really", "really", "want"],
            ],
        }
    )

    expected_df = pd.DataFrame(
        {
            "object_id": [
                "id_1",
                "id_1",
                "id_1",
                "id_1",
                "id_1",
                "id_1",
                "id_2",
                "id_2",
                "id_2",
                "id_2",
            ],
            "features": [
                "so",
                "tell",
                "me",
                "what",
                "you",
                "want",
                "what",
                "you",
                "really",
                "want",
            ],
            "features_count": [1, 1, 1, 1, 1, 1, 1, 1, 2, 1],
        }
    )

    actual_df = feature.explode_features(input_df)

    pd.testing.assert_frame_equal(
        actual_df.sort_values(by="features").reset_index(drop=True),
        expected_df.sort_values(by="features").reset_index(drop=True),
    )


@mock.patch(
    "phoenix.tag.feature.SFLM_NECESSARY_FEATURES_COLUMNS",
    ["object_id", "object_type", "language", "features"],
)
def test_keep_neccesary_columns_sflm():
    """Test keep_necessary columns for sflm."""
    input_df = pd.DataFrame(
        {
            "object_id": ["id_1"],
            "object_type": ["obj_type_1"],
            "language": ["lang_1"],
            "features": ["feature_1"],
            "unnecessary_column": ["unnecessary"],
        }
    )

    expected_df = pd.DataFrame(
        {
            "object_id": ["id_1"],
            "object_type": ["obj_type_1"],
            "language": ["lang_1"],
            "features": ["feature_1"],
        }
    )

    actual_df = feature.keep_neccesary_columns_sflm(input_df)

    pd.testing.assert_frame_equal(expected_df, actual_df)
