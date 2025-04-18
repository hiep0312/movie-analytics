import os
from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

dataset = 'rounakbanik/the-movies-dataset'
download_path = 'the-movies-dataset'
