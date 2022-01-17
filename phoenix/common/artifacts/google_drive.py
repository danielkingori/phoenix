"""Getting and persisting dataframes from and to Google Sheets."""
from typing import Any, Dict, List

import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_client():
    """Get google drive client."""
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def get_files(client, folder_id: str):
    """Get the files in a folder."""
    files: List[Dict[str, Any]] = []
    page_token = None
    q = ""
    if folder_id:
        q += f"'{folder_id}' in parents"
    while True:
        response = (
            client.files()
            .list(
                q=q,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, trashed, parents)",
                pageToken=page_token,
            )
            .execute()
        )
        for file in response.get("files", []):
            files.append(
                {
                    attrib: file.get(attrib)
                    for attrib in ["name", "id", "mimeType", "trashed", "parents"]
                }
            )
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    return files
