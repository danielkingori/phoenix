"""Getting and persisting dataframes from and to Google Sheets."""
from typing import List

import os

import gspread
import pandas as pd


def get_client():
    """Get gspread client."""
    gspread_client = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    return gspread_client


def create_sheet(gspread_client, folder_id: str, sheet_name: str, worksheet_names: List[str]):
    """Create a sheet and worksheets within sheet.

    To further add or edit worksheets to a sheet already created, one should do so manually either
    via browser or use gspread directly.
    """
    sheet = gspread_client.create(sheet_name, folder_id=folder_id)
    for worksheet_name in worksheet_names:
        sheet.add_worksheet(title=worksheet_name, rows=0, cols=0)


def _get_worksheet(
    gspread_client, folder_id: str, sheet_name: str, worksheet_name: str
) -> gspread.Worksheet:
    """Get worksheet object for given folder ID, sheet name, and worksheet name."""
    sheet = gspread_client.open(sheet_name, folder_id)
    return sheet.worksheet(worksheet_name)


def persist(
    gspread_client, folder_id: str, sheet_name: str, worksheet_name: str, df: pd.DataFrame
):
    """Persist dataframe to worksheet of sheet.

    WARNING: Sheet names must be unique within folder for folder ID given otherwise it will pick
    the "first" sheet with given name it finds in the folder. Note that this is a patched version
    of `gspread` allowing to use `folder_id` in `open` which massively reduces sheet name clash
    issues.
    """
    worksheet = _get_worksheet(gspread_client, folder_id, sheet_name, worksheet_name)
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def get(gspread_client, folder_id: str, sheet_name: str, worksheet_name: str) -> pd.DataFrame:
    """Get dataframe of worksheet of sheet in folder."""
    worksheet = _get_worksheet(gspread_client, folder_id, sheet_name, worksheet_name)
    return pd.DataFrame(worksheet.get_all_records())
