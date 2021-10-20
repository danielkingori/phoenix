"""Google Drive artifacts."""
import mock

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
