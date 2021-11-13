"""Patch for gspread client, to allow `open` with Sheet name and parent folder_id.

Core code taken from: https://github.com/burnash/gspread/blob/master/gspread/client.py
Google API reference: https://developers.google.com/drive/api/v3/ref-search-terms#file_properties
"""
from typing import Optional

from gspread import Spreadsheet, client
from gspread.exceptions import SpreadsheetNotFound
from gspread.urls import DRIVE_FILES_API_V3_URL
from gspread.utils import finditem


def list_spreadsheet_files(self, title: Optional[str] = None, folder_id: Optional[str] = None):
    """List all files of spreadsheet type from Drive."""
    files = []
    page_token = ""
    url = DRIVE_FILES_API_V3_URL

    q = 'mimeType="application/vnd.google-apps.spreadsheet"'
    if title:
        q += ' and name = "{}"'.format(title)
    if folder_id:
        q += ' and parents in "{}"'.format(folder_id)

    params = {
        "q": q,
        "pageSize": 1000,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "fields": "kind,nextPageToken,files(id,name,createdTime,modifiedTime)",
    }

    while page_token is not None:
        if page_token:
            params["pageToken"] = page_token

        res = self.request("get", url, params=params).json()
        files.extend(res["files"])
        page_token = res.get("nextPageToken", None)

    return files


def open(self, title, folder_id: Optional[str] = None):
    """Opens a spreadsheet.

    Args:
        title (str): A title of a spreadsheet.
        folder_id (str): The ID of a Drive folder within which spreadsheet will be found from.

    Returns:
        gspread.models.Spreadsheet

    If there's more than one spreadsheet with same title (within the same folder, including root
    folder if no folder_id given) the first one will be opened.

    Raises:
        gspread.SpreadsheetNotFound: if no spreadsheet with specified `title` is found.

    >>> gc.open('My fancy spreadsheet')
    """
    try:
        properties = finditem(
            lambda x: x["name"] == title,
            self.list_spreadsheet_files(title, folder_id),
        )

        # Drive uses different terminology
        properties["title"] = properties["name"]

        return Spreadsheet(self, properties)
    except StopIteration:
        raise SpreadsheetNotFound


client.Client.list_spreadsheet_files = list_spreadsheet_files
client.Client.open = open
