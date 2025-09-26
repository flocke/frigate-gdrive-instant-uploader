import os

from webdav4.client import Client

from src.uploader.generic import GenericUploader

class WebDavUploader(GenericUploader):
    def __init__(self):
        host = os.getenv('WEBDAV_HOST')
        user = os.getenv('WEBDAV_USER')
        password = os.getenv('WEBDAV_PASSWORD')

        self.client = Client(host, auth=(user, password))