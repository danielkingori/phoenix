"""Artifacts Google Sheets interface."""
from typing import Dict, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/drive"]


class GoogleDriveInterface:
    """Get an interface with Google Drive."""

    def __init__(self, creds_loc: Optional[str] = None):
        if not creds_loc:
            creds_loc = "../../../.phoenix-sheet-integration-SA-key.json"
        self.creds = Credentials.from_service_account_file(creds_loc, scopes=SCOPES)
        self.drive_service = build("drive", "v3", credentials=self.creds)
        self.sheet_service = build("sheets", "v4", credentials=self.creds).spreadsheets()

    def get_files_in_folder(self, folder_id) -> Dict[str, str]:
        """Get the files in a folder with name and id."""
        name_to_id_dict = {}

        response = self.drive_service.files().list(q=f"'{folder_id}' in parents").execute()
        for file in response.get("files", []):
            name_to_id_dict[file.get("name")] = file.get("id")

        return name_to_id_dict

    def get_sheet_metadata(self, spreadsheet_id: str) -> Dict:
        """Get a spreadsheet's metadata."""
        query = self.sheet_service.get(spreadsheetId=spreadsheet_id)
        result = query.execute()
        sheet_property_list = result.get("sheets", [])
        metadata_dict = {}
        for sheet in sheet_property_list:
            title = result.get("properties", {}).get("title", "")
            sheet_name = sheet.get("properties", {}).get("title", "")
            row_count = sheet.get("properties", {}).get("gridProperties", {}).get("rowCount")
            col_count = sheet.get("properties", {}).get("gridProperties", {}).get("columnCount")
            metadata_dict[sheet_name] = {
                "canonical_parent_name": title,
                "len_rows": row_count,
                "len_cols": col_count,
            }

        return metadata_dict

    def get_sheet_data(self, spreadsheet_id: str, range: str):
        """Get a spreadsheet by its id."""
        query = self.sheet_service.values().get(spreadsheetId=spreadsheet_id, range=range)
        result = query.execute()
        return result
