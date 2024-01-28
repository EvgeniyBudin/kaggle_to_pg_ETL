import os

from kaggle.api.kaggle_api_extended import KaggleApi


class KaggleManager:
    def __init__(self):
        self.path_to_load = os.path.abspath(os.path.dirname(__file__))

    def download(self, dataset_path: str, file: str) -> None:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_file(dataset_path, file_name=file, path=self.path_to_load)
