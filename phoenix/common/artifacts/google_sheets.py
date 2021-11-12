"""Getting and persisting dataframes from and to Google Sheets."""
import os

import gspread
import pandas as pd


def get_client():
    """Get gspread client."""
    gspread_client = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    return gspread_client


def persist(
    gspread_client, folder_id: str, sheet_name: str, worksheet_name: str, df: pd.DataFrame
):
    """Persist dataframe to worksheet of sheet.

    WARNING: Sheet names must be unique within all Sheets accessible to Google account (likely a
    Service Account), otherwise it will pick the "first" sheet with given name it finds.
    """
    sheet = gspread_client.create(sheet_name, folder_id=folder_id)
    worksheet = sheet.add_worksheet(title=worksheet_name, rows=0, cols=0)
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def get(gspread_client, folder_id: str, sheet_name: str, worksheet_name: str) -> pd.DataFrame:
    """Get dataframe of worksheet of sheet in folder."""
    sheet = gspread_client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)
    return pd.DataFrame(worksheet.get_all_records())
