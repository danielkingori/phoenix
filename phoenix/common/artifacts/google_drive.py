"""Artifacts Google Sheets interface."""
from typing import Optional

from google.oauth2.service_account import Credentials


SCOPES = ["https://www.googleapis.com/auth/drive"]


class GoogleDriveInterface:
    """Get an interface with Google Drive."""

    def __init__(self, creds_loc: Optional[str] = None):
        if not creds_loc:
            creds_loc = "../../../.phoenix-sheet-integration-SA-key.json"
        self.creds = Credentials.from_service_account_file(creds_loc, scopes=SCOPES)
