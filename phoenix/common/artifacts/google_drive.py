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


def convert_row_col_to_range(
    sheet_name: str, max_row: int, max_col: int, min_row: int = 1, min_col: int = 1
) -> str:
    """Converts number of rows and columns to R1C1 notation.

    Google sheets natively uses letters for column coordinates, R1C1 notation allows us to pick
    a range of rows and columns using numbers only.

    Args:
        sheet_name (str): name of the sheet within the spreadsheet - can be one of multiple sheets
            under a spreadsheet.
        max_row (int): max row to pull
        max_col (int): max column to pull
        min_row (int): First row to pull, defaults to 1
        min_col (int): First column to pull, defaults to 1
    """
    range = f"{sheet_name}!R{min_row}C{min_col}:R{max_row}C{max_col}"
    return range
