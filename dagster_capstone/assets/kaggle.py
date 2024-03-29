# kaggle datasets download -d -unzip yelp-dataset/yelp-dataset

import os
import requests
import subprocess
from . import constants
from dagster import asset
import duckdb
import pandas as pd
from ..partitions import monthly_partition, weekly_partition

@asset(
)
def kaggle_file(context):
    """
      The raw json files for the Kaggle dataset. Sourced from the Kaggle API.
    """
    command = f"kaggle datasets download -d {constants.KAGGLE_DATASET} -p {constants.KAGGLE_FILE_PATH} --unzip"
    try:
        # Execute the command
        subprocess.run(command, shell=True, check=True)
        print("Yelp dataset downloaded and unzipped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while downloading the Yelp dataset: {e}")

