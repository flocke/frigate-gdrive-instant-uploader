import io
import logging
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from src.uploader.generic import GenericUploader

SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
GOOGLE_ACCOUNT_TO_IMPERSONATE = os.getenv('GOOGLE_ACCOUNT_TO_IMPERSONATE')

SCOPES = ['https://www.googleapis.com/auth/drive']

class GoogleDriverUploader(GenericUploader):
    def __init__(self):
        if GOOGLE_ACCOUNT_TO_IMPERSONATE:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES, subject=GOOGLE_ACCOUNT_TO_IMPERSONATE)
        else:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        self.service = build('drive', 'v3', credentials=credentials)

    def find_or_create_folder(self, name, parent_id=None) -> str:
        try:
            query = f"name='{name}' and mimeType='application/vnd.google-apps.folder'"
            if parent_id:
                query += f" and parents in '{parent_id}'"
            results = self.service.files().list(q=query, spaces='drive', fields='files(id)').execute()
            folder = results.get('files', [])
            if not folder:
                folder_metadata = {
                    'name': name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [parent_id] if parent_id else []
                }
                folder = self.service.files().create(body=folder_metadata, fields='id').execute()
                return folder.get('id')
            else:
                return folder[0]['id']

        except HttpError as error:
            logging.error(f"An error occurred: {error}")
            return None

    def create_event_folder(self, folder_path : Path) -> str | None:
        frigate_folder_id = None

        for part in folder_path:
            frigate_folder_id = self.find_or_create_folder(part, frigate_folder_id)

            if not frigate_folder_id:
                logging.error(f"Failed to find or create folder: {part}")
                return None
        
        return frigate_folder_id
    
    def upload_video_file(self, fh : io.FileIO, folder_info : str, filename : str, _) -> bool:
        media = MediaIoBaseUpload(fh, mimetype='video/mp4', resumable=True)
        file_metadata = {'name': filename, 'parents': [ folder_info ]}
        
        try:
            file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            if 'id' in file:
                logging.info(f"Video {filename} successfully uploaded to Google Drive with ID: {file['id']}.")
                return True
            else:
                logging.error(f"Failed to upload video {filename} to Google Drive. No file ID returned.")
                return False
        except HttpError as error:
            logging.error(f"Error uploading to Google Drive: {error}")
            return False
