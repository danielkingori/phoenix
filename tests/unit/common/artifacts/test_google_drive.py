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
