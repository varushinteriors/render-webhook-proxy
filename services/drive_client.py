from __future__ import annotations

import io
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload


class DriveClient:
    def __init__(
        self,
        parent_folder_id: Optional[str],
        credentials_path: Optional[str] = None,
        credentials_info: Optional[dict] = None,
    ):
        self.credentials_path = credentials_path
        self.credentials_info = credentials_info
        self.parent_folder_id = parent_folder_id
        self._service = None

    def ready(self) -> bool:
        return bool(self.parent_folder_id and (self.credentials_path or self.credentials_info))

    def _service_client(self):
        if self._service is None:
            scopes = ["https://www.googleapis.com/auth/drive"]
            if self.credentials_info:
                creds = service_account.Credentials.from_service_account_info(
                    self.credentials_info, scopes=scopes
                )
            else:
                creds = service_account.Credentials.from_service_account_file(
                    self.credentials_path, scopes=scopes
                )
            self._service = build("drive", "v3", credentials=creds)
        return self._service

    def ensure_folder(self, name: str) -> Optional[dict]:
        if not self.ready():
            return None
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [self.parent_folder_id],
        }
        try:
            folder = (
                self._service_client()
                .files()
                .create(body=metadata, fields="id, name")
                .execute()
            )
            # Make link-shareable (viewer)
            self._service_client().permissions().create(
                fileId=folder["id"], body={"role": "commenter", "type": "anyone"}
            ).execute()
            return folder
        except HttpError as exc:
            print(f"DRIVE UPLOAD ERROR: {exc}")
            return None

    def upload_bytes(
        self,
        folder_id: str,
        filename: str,
        mime_type: str,
        data: bytes,
    ) -> Optional[dict]:
        if not self.ready():
            return None
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=True)
        metadata = {"name": filename, "parents": [folder_id]}
        try:
            file = (
                self._service_client()
                .files()
                .create(body=metadata, media_body=media, fields="id, name, webViewLink")
                .execute()
            )
            print(f"DRIVE UPLOAD SUCCESS: {file}")
            return file
        except HttpError as exc:
            print(f"DRIVE UPLOAD ERROR: {exc}")
            return None

    @staticmethod
    def folder_link(folder_id: str) -> str:
        return f"https://drive.google.com/drive/folders/{folder_id}"
