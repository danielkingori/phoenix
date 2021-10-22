"""Google Drive artifacts."""
import mock
import pandas as pd

from phoenix.common.artifacts import google_drive


class MockGoogleDriveInterface(google_drive.GoogleDriveInterface):
    """Mock the Google Drive Interface leaving mock services instead of real services."""

    def __init__(self):
        self.creds = "mock_credentials_instance"
        self.drive_service = mock.MagicMock()
        self.sheet_service = mock.MagicMock()


def test_google_drive_interface_get_files_in_folder():
    mock_gdi_obj = MockGoogleDriveInterface()
    expected_name_to_id_dict = {"mock_name": "mock_id"}

    mock_execute_method = mock.MagicMock()
    mock_execute_method.return_value = {"files": [{"name": "mock_name", "id": "mock_id"}]}
    mock_gdi_obj.drive_service.files().list().execute = mock_execute_method

    actual_name_to_id_dict = mock_gdi_obj.get_files_in_folder("mock_id")
    assert expected_name_to_id_dict == actual_name_to_id_dict


def test_get_sheet_metadata():
    """Test that get_sheet_metadata returns correct metadata."""
    query_result = {
        "spreadsheetId": "1-VVofuTrtw24sysmTtIH5RjgcN_CRmIhK-bWzsxfeUU",
        "properties": {
            "title": "test_sheets_phoenix",
            "locale": "en_GB",
            "autoRecalc": "ON_CHANGE",
            "timeZone": "Europe/Paris",
            "defaultFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}},
        },
        "sheets": [
            {
                "properties": {
                    "sheetId": 0,
                    "title": "Sheet1",
                    "index": 0,
                    "sheetType": "GRID",
                    "gridProperties": {"rowCount": 999, "columnCount": 26},
                }
            }
        ],
        "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/1-VVofuTrtw24sysmTtIH5RjgcN_CRmIhK-bWzsxfeUU/edit",  # noqa
    }
    mock_gdi_obj = MockGoogleDriveInterface()
    expected_metadata_dict = {
        "Sheet1": {"canonical_parent_name": "test_sheets_phoenix", "len_rows": 999, "len_cols": 26}
    }
    mock_gdi_obj.sheet_service.get().execute.return_value = query_result

    actual_dict = mock_gdi_obj.get_sheet_metadata("mock_id")
    assert actual_dict == expected_metadata_dict


def test_get_sheet_data_as_df_infer_col_name():
    """Test conversion of sheet data to dataframe."""
    mock_gdi_obj = MockGoogleDriveInterface()
    sheet_data = {
        "range": "Sheet1!A1:R2",
        "majorDimension": "ROWS",
        "values": [
            [
                "index",
                "object_id",
                "object_type",
            ],
            [
                "0",
                "100044142351096-13f3e41944a37145",
                "facebook_post",
            ],
        ],
    }
    mock_get_sheet_data = mock.MagicMock()
    mock_get_sheet_data.return_value = sheet_data
    # https://github.com/python/mypy/issues/2427 - mypy has problems with assigning functions to
    # MagicMock
    mock_gdi_obj.get_sheet_data = mock_get_sheet_data  # type: ignore

    expected_df = pd.DataFrame(
        {
            "index": ["0"],
            "object_id": ["100044142351096-13f3e41944a37145"],
            "object_type": ["facebook_post"],
        }
    )
    actual_df = mock_gdi_obj.get_sheet_data_as_df("mock_id", "some_range")
    # check_names=False is needed because in creating the expected df, the column names are
    # ["index", "object_id", "object_type"], but expected_df.columns.names = None, whereas the
    # method used in get_sheet_data_as_df sets the actual_df.columns.names = 0. This has no
    # impact on the working of the function.
    pd.testing.assert_frame_equal(actual_df, expected_df, check_names=False)
    mock_get_sheet_data.assert_called_with("mock_id", "some_range")


def test_get_sheet_data_as_df_no_col_name():
    """Test conversion of sheet data to dataframe."""
    mock_gdi_obj = MockGoogleDriveInterface()
    sheet_data = {
        "range": "Sheet1!A1:R2",
        "majorDimension": "ROWS",
        "values": [
            [
                "index",
                "object_id",
                "object_type",
            ],
            [
                "0",
                "100044142351096-13f3e41944a37145",
                "facebook_post",
            ],
        ],
    }
    mock_get_sheet_data = mock.MagicMock()
    mock_get_sheet_data.return_value = sheet_data
    # https://github.com/python/mypy/issues/2427 - mypy has problems with assigning functions to
    # MagicMock
    mock_gdi_obj.get_sheet_data = mock_get_sheet_data  # type: ignore

    expected_df = pd.DataFrame(
        [
            [
                "index",
                "object_id",
                "object_type",
            ],
            [
                "0",
                "100044142351096-13f3e41944a37145",
                "facebook_post",
            ],
        ]
    )
    actual_df = mock_gdi_obj.get_sheet_data_as_df("mock_id", "some_range", False)
    pd.testing.assert_frame_equal(actual_df, expected_df)
    mock_get_sheet_data.assert_called_with("mock_id", "some_range")


def test_convert_row_col_to_range_default():
    """Test the conversion of rows and columns into a range understandable by Google Sheets."""
    expected_value = "MySheet!R1C1:R25C16"
    actual_value = google_drive.convert_row_col_to_range("MySheet", 25, 16)
    assert actual_value == expected_value


def test_convert_row_col_to_range():
    """Test the conversion of rows and columns into a range understandable by Google Sheets."""
    expected_value = "MySheet!R2C3:R25C16"
    actual_value = google_drive.convert_row_col_to_range("MySheet", 25, 16, 2, 3)
    assert actual_value == expected_value


def test__df_to_call_body():
    input_df = pd.DataFrame(
        {
            "index": ["0", "1"],
            "object_id": ["100044142351096-13f3e41944a37145", "999999"],
            "object_type": ["facebook_post", "facebook_post"],
        }
    )
    sheet_name = "test_sheet"

    expected_body = {
        "range": "test_sheet!R1C1:R3C4",
        "values": [
            ["index", "object_id", "object_type"],
            ["0", "100044142351096-13f3e41944a37145", "facebook_post"],
            ["1", "999999", "facebook_post"],
        ],
    }

    actual_body = google_drive.GoogleDriveInterface._df_to_call_body(input_df, sheet_name)
    assert actual_body == expected_body


def test_update_sheet_with_df():
    mock_gdi = MockGoogleDriveInterface()

    metadata_dict = {
        "Sheet1": {"canonical_parent_name": "test_sheets_phoenix", "len_rows": 999, "len_cols": 26}
    }
    mock_get_sheet_data = mock.MagicMock()
    mock_get_sheet_data.return_value = metadata_dict
    # https://github.com/python/mypy/issues/2427 - mypy has problems with assigning functions to
    # MagicMock
    mock_gdi.get_sheet_metadata = mock_get_sheet_data  # type: ignore

    call_body = {"range": "some_range", "values": "list_of_values"}
    mock__df_to_call_body = mock.MagicMock()
    mock__df_to_call_body.return_value = call_body
    # https://github.com/python/mypy/issues/2427 - mypy has problems with assigning functions to
    # MagicMock
    mock_gdi._df_to_call_body = mock__df_to_call_body  # type: ignore

    input_df = pd.DataFrame(
        {
            "index": ["0", "1"],
            "object_id": ["100044142351096-13f3e41944a37145", "999999"],
            "object_type": ["facebook_post", "facebook_post"],
        }
    )
    input_spreadsheet_id = "some_id"
    mock_gdi.update_sheet_with_df(input_df, input_spreadsheet_id)

    mock_get_sheet_data.assert_called_with("some_id")
    mock__df_to_call_body.assert_called_with(input_df, "Sheet1", True)
    mock_gdi.sheet_service.values().batchUpdate.assert_called_with(
        spreadsheetId="some_id",
        body={
            "value_input_option": "USER_ENTERED",
            "data": {"range": "some_range", "values": "list_of_values"},
        },
    )
    mock_gdi.sheet_service.values().batchUpdate().execute.assert_called_once()
