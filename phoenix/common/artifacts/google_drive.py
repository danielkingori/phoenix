"""Getting and persisting dataframes from and to Google Sheets."""
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
