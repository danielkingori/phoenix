"""Artifacts Google Sheets interface."""
from typing import Any, Dict, Optional

import pandas as pd
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

    def get_sheet_data_as_df(
        self, spreadsheet_id: str, range: str, use_top_row_as_col_names: bool = True
    ) -> pd.DataFrame:
        """Gets a google sheet and returns it as a DataFrame.

        Args:
            spreadsheet_id (str): ID of the spreadsheet to pull.
            range (str): range of columns and rows to pull from the spreadsheet.
            use_top_row_as_col_names (bool): Use the first row of data as column names in df.
        """
        sheet_data = self.get_sheet_data(spreadsheet_id, range)
        vals = sheet_data.get("values", [])
        num_rows = len(vals)

        if use_top_row_as_col_names and num_rows > 1:
            df = pd.DataFrame(vals)
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
        elif not use_top_row_as_col_names and num_rows > 0:
            df = pd.DataFrame(vals)
        else:
            raise ValueError(f"Unable to make dataframe: Sheet only had {num_rows} rows.")

        return df

    @staticmethod
    def _df_to_call_body(
        df: pd.DataFrame,
        sheet_name: str,
        col_name_as_first_row: bool = True,
        offset_row: int = 0,
        offset_col: int = 0,
    ) -> Dict[str, Any]:
        """Create the body of a GSheet api call.

        Args:
            df (pd.DataFrame): dataframe to get data from
            sheet_name (str): Name of the sheet to update on -can be one of multiple sheets
                under a spreadsheet.
            col_name_as_first_row (bool): Use the dataframe's columns as first row. default True.
            offset_row (int): Start inputting data from an offset from the first row. default 0
            offset_col (int): Start inputting data from an offset from the first column. default 0
        """
        rows, cols = df.shape
        # Add any offsets
        rows += offset_row
        cols += offset_col
        if col_name_as_first_row:
            rows += 1
            cols += 1
        offset_row += 1
        offset_col += 1
        sheet_range = convert_row_col_to_range(sheet_name, rows, cols, offset_row, offset_col)

        data_values = df.values.tolist()
        if col_name_as_first_row:
            data_col_names = [df.columns.values.tolist()]
            data_col_names.extend(data_values)
            data_values = data_col_names

        body = {
            "range": sheet_range,
            "values": data_values,
        }
        return body

    def update_sheet_with_df(
        self,
        df: pd.DataFrame,
        spreadsheet_id: str,
        sheet_name: Optional[str] = None,
        clear_sheet: bool = False,
        col_name_as_first_row: bool = True,
    ):
        """Updates a google sheet by using a DataFrame.

        Args:
            df (pd.DataFrame): Dataframe you want to use to update a gsheet.
            spreadsheet_id (str): ID of the spreadsheet you want to update.
            sheet_name (Optional[str]): If the spreadsheet contains multiple.
            clear_sheet (bool): Clear the sheet of all other data before updating. default False.
            col_name_as_first_row (bool): Use the dataframe's columns as first row. default True.
        """
        metadata_dict = self.get_sheet_metadata(spreadsheet_id)
        if not sheet_name:
            # Take the first sheet found if no sheet name is provided.
            sheet_name = next(iter(metadata_dict))
        sheet_metadata = metadata_dict[sheet_name]

        if clear_sheet:
            clear_range = convert_row_col_to_range(
                sheet_name, sheet_metadata["len_rows"], sheet_metadata["len_cols"]  # type: ignore
            )
            self.sheet_service.values().clear(
                spreadsheetId=spreadsheet_id, range=clear_range
            ).execute()

        batch_update_values_request_body = {
            "value_input_option": "USER_ENTERED",
            "data": self._df_to_call_body(df, sheet_name, col_name_as_first_row),  # type: ignore
        }

        request = self.sheet_service.values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=batch_update_values_request_body
        )

        response = request.execute()
        return response


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
