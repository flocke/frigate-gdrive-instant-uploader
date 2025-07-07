import io
import logging
import os
import requests
import ssl
import tempfile
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path

import pytz

from src import database
from src.frigate_api import generate_video_url

TIMEZONE = os.getenv('TIMEZONE', 'Europe/Istanbul')
UPLOAD_DIR = os.getenv('UPLOAD_DIR')

@dataclass
class EventInfo:
    """
        Simple dataclass to store event information for uploading
    """
    event_id : str
    camera_name : str
    start_time : str

    start_time_utc : datetime = field(init=False)
    start_time_local : datetime = field(init=False)

    def __post_init__(self):
        self.start_time_utc = datetime.fromtimestamp(self.start_time, pytz.utc)
        self.start_time_local = self.start_time_utc.astimezone(pytz.timezone(TIMEZONE))

    def generate_filename(self) -> str:
        """
            Generate a video filename for the event
        """
        return f"{self.start_time_local.strftime('%Y-%m-%d-%H-%M-%S')}__{self.camera_name}__{self.event_id}.mp4"

    def generate_folder_path(self, base : Path) -> Path:
        """
            Generate a folder path for the event
        """
        return base.joinpath(self.start_time_local.year).joinpath(self.start_time_local.month).joinpath(self.start_time_local.day)

    @classmethod
    def from_event_dict(cls, event_dict):
        """
            Construct an EventInfo object from a dict
        """
        return EventInfo(
            event_id=event_dict["id"],
            camera_name=event_dict["camera"],
            start_time=event_dict["start_time"]
        )


class GenericUploader:
    """ Generic uploader class intended as base for specific uploaders """

    def create_event_folder(self, folder_path : Path) -> object | None:
        """
            Prototype for a function to create a folder for the upload.
            Needs to be implemented by the specific uploader class.
        """
        raise RuntimeError("This function needs to be implemented by an uploader class!")

    def upload_video_file(self, fh : io.FileIO, folder_info : object, filename : str, event_info : EventInfo) -> bool:
        """
            Prototype for a function to upload a video file to a folder.
            Needs to be implemented by the specific uploader class.
        """
        raise RuntimeError("This function needs to be implemented by an uploader class!")

    @property
    def upload_folder(self) -> Path:
        """
            Path to the base upload folder.
            Can be overwritten in a specific uploader.
        """
        return Path(UPLOAD_DIR)

    def upload(self, event, frigate_url):
        """
            Main worker function to perform the actual upload of an event
        """
        event_info = EventInfo.from_event_dict(event)

        filename = event_info.generate_filename()
        folder_path = event_info.generate_folder_path(self.upload_folder)

        video_url = generate_video_url(frigate_url, event_info.event_id)

        folder_info = self.create_event_folder(folder_path)
        
        if not folder_info:
            return False

        try:
            with tempfile.TemporaryFile() as fh:
                response = requests.get(video_url, stream=True, timeout=300)
                if response.status_code == 200:
                    for chunk in response.iter_content(chunk_size=8192):
                        fh.write(chunk)
                    fh.seek(0)

                    return self.upload_video_file(fh, folder_info, filename, event_info)
                elif response.status_code == 500 and response.json().get('message') == "Could not create clip from recordings":
                    logging.warning(f"Clip not found for event {event_info.event_id}.")
                    if database.select_tries(event_info.event_id) >= 10:
                        database.update_event(event_info.event_id, 0, retry=0)
                        logging.error(f"Clip creation failed for {event_info.event_id}. "
                                    f"Couldn't download its clip from {generate_video_url(frigate_url, event_info.event_id)}. "
                                    f"Marking as non-retriable.")
                    return False
                logging.error(f"Could not download video from {video_url}. Status code: {response.status_code}")
                return False

        except (requests.RequestException, ssl.SSLError) as e:
            logging.error(f"Error downloading video from {video_url}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
            return False
