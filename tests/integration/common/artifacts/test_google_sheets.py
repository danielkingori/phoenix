"""Integration tests for Google Sheets interface.

Requires GCP Service Account (or use) credentials (i.e. JSON key) path added as env var
`GOOGLE_APPLICATION_CREDENTIALS`.
"""
from typing import Any, Dict, List, Optional

import logging
import re
import uuid

import pandas as pd
import pytest
from googleapiclient import discovery

from phoenix.common.artifacts import google_sheets


logger = logging.getLogger(__name__)


def create_drive_folder(drive_service, folder_name: str, parent_id: Optional[str] = None) -> str:
    """Create a folder on Drive returning the newly created folder's ID."""
    body: Dict[str, Any] = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    root_folder = drive_service.files().create(body=body).execute()
    return root_folder["id"]


def delete_drive_object(drive_service, file_id: str) -> None:
    """Delete object with given ID from within Drive.

    Note that deleting a folder deletes the folder's contents.
    """
    drive_service.files().delete(fileId=file_id).execute()


def list_all_drive_objects(drive_service) -> List[Dict[str, Any]]:
    """List all objects found in Drive.

    Utility function for ensuring that test teardown is working as intended.
    """
    files: List[Dict[str, Any]] = []
    page_token = None
    while True:
        response = (
            drive_service.files()
            .list(
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


@pytest.fixture
def tmp_google_drive_folder_id(request):
    """Create a temporary Google Drive folder and return its ID for the test to use.

    This implementation is not foolproof and can leave temporary objects around, but this is very
    rare (i.e. if you keyboard interrupted the test post setup but pre teardown) and this isn't
    expected to incur costs.

    Info level logging is added to output current files existing in Drive.
    """
    test_name = re.sub(r"[\W]", "_", request.node.name)[:40]
    tmp_folder_name = f"tmp_phoenix_integration_testing_{test_name}_{str(uuid.uuid4())[:40]}"

    drive_service = discovery.build("drive", "v3")
    logger.info(list_all_drive_objects(drive_service))
    tmp_folder_id = create_drive_folder(drive_service, tmp_folder_name)
    logger.info(list_all_drive_objects(drive_service))
    yield tmp_folder_id
    logger.info(list_all_drive_objects(drive_service))
    delete_drive_object(drive_service, file_id=tmp_folder_id)
    # Note, this next log of all drive objects most likely will still show the sheet present in the
    # returned files, but this is a race condition; the transitive deletion of the sheet from its
    # parent being deleted has not yet happened. Add a `sleep(30)` and you will see that the list
    # of returned files is empty again.
    logger.info(list_all_drive_objects(drive_service))


@pytest.mark.auth
def test_persist_google_sheet(tmp_google_drive_folder_id):
    """Test persisting (and getting) a dataframe to (and from) a Google Sheet.

    This test uses the real Google Drive to persist Sheets to.
    """
    in_df = pd.DataFrame(
        {
            "col A": [1],
        }
    )
    client = google_sheets.get_client()
    sheet_name = "test_sheet"
    worksheet_name = "test_worksheet"
    google_sheets.create_sheet(client, tmp_google_drive_folder_id, sheet_name, [worksheet_name])
    google_sheets.persist(client, tmp_google_drive_folder_id, sheet_name, worksheet_name, in_df)

    out_df = google_sheets.get(client, tmp_google_drive_folder_id, sheet_name, worksheet_name)
    pd.testing.assert_frame_equal(out_df, in_df)


@pytest.mark.auth
def test_persist_overwrite_google_sheet(tmp_google_drive_folder_id):
    """Test overwriting a dataframe to (and from) a Google Sheet.

    This test uses the real Google Drive to persist Sheets to.
    """
    in_df = pd.DataFrame(
        {
            "col A": [1],
        }
    )
    client = google_sheets.get_client()
    sheet_name = "test_sheet"
    worksheet_name = "test_worksheet"
    google_sheets.create_sheet(client, tmp_google_drive_folder_id, sheet_name, [worksheet_name])
    google_sheets.persist(client, tmp_google_drive_folder_id, sheet_name, worksheet_name, in_df)

    out_df = google_sheets.get(client, tmp_google_drive_folder_id, sheet_name, worksheet_name)
    pd.testing.assert_frame_equal(out_df, in_df)

    in_df = pd.DataFrame(
        {
            "col B": [1, 2],
        }
    )
    google_sheets.persist(client, tmp_google_drive_folder_id, sheet_name, worksheet_name, in_df)

    out_df = google_sheets.get(client, tmp_google_drive_folder_id, sheet_name, worksheet_name)
    pd.testing.assert_frame_equal(out_df, in_df)


tmp_google_drive_folder_id_2 = tmp_google_drive_folder_id


@pytest.mark.auth
def test_patched_open(tmp_google_drive_folder_id, tmp_google_drive_folder_id_2):
    """Test patched `open` and ensure folder ID is used when opening Sheet using its name.

    This test fails without the patch, as unpatched `open` will pick the first sheet which matches
    name irrespective of sheet locations.

    This test uses the real Google Drive to persist Sheets to.
    """
    sheet_name = "test_sheet"
    worksheet_name = "test_worksheet"

    client = google_sheets.get_client()

    in_df = pd.DataFrame(
        {
            "col A": [1],
        }
    )
    google_sheets.create_sheet(client, tmp_google_drive_folder_id, sheet_name, [worksheet_name])
    google_sheets.persist(client, tmp_google_drive_folder_id, sheet_name, worksheet_name, in_df)

    in_df_2 = pd.DataFrame(
        {
            "col B": [2],
        }
    )
    google_sheets.create_sheet(client, tmp_google_drive_folder_id_2, sheet_name, [worksheet_name])
    google_sheets.persist(
        client, tmp_google_drive_folder_id_2, sheet_name, worksheet_name, in_df_2
    )

    out_df = google_sheets.get(client, tmp_google_drive_folder_id, sheet_name, worksheet_name)
    out_df_2 = google_sheets.get(client, tmp_google_drive_folder_id_2, sheet_name, worksheet_name)
    pd.testing.assert_frame_equal(out_df, in_df)
    pd.testing.assert_frame_equal(out_df_2, in_df_2)
