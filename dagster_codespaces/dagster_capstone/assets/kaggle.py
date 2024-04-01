# kaggle datasets download -d -unzip yelp-dataset/yelp-dataset

import boto3
import os
from pathlib import Path
import subprocess
from ..assets import constants
import pandas as pd
from dagster import asset, IOManager, io_manager, Field

class KaggleFileManager(IOManager):
    def handle_output(self, context, obj):
        # obj here would be the local file path to the downloaded dataset
        s3 = boto3.client('s3')
        bucket_name = context.resource_config['s3_bucket']
        s3_key_prefix = context.resource_config.get('s3_key_prefix', '')
        
        # Assuming obj is the directory containing the downloaded files
        for file_name in os.listdir(obj):
            file_path = os.path.join(obj, file_name)
            if os.path.isfile(file_path):
                s3_key = f"{s3_key_prefix}{file_name}"
                s3.upload_file(file_path, bucket_name, s3_key)
                context.log.info(f"Uploaded {file_name} to s3://{bucket_name}/{s3_key}")

    def load_input(self, context):
        # Loading input is not relevant for the download asset, but you could implement
        # downloading from S3 here if needed
        pass

@io_manager(config_schema={
    "s3_bucket": Field(str), 
    "s3_key_prefix": Field(str, is_required=False, default_value="")
    })
def kaggle_file_manager():
    return KaggleFileManager()


@asset(
    io_manager_key='kaggle_io_manager',
    config_schema={
        "kaggle_dataset": Field(str, default_value='yelp-dataset/yelp-dataset'),
        "file_path": Field(str, default_value=str(Path(__file__).parents[3] / 'data' / 'raw' / 'kaggle'))
    }
)
def kaggle_file(context):
    dataset = context.op_config["kaggle_dataset"]
    file_path = context.op_config["file_path"]

    # Check if the dataset directory already exists and has data
    if not os.path.exists(file_path) or not os.listdir(file_path):
        command = f"kaggle datasets download -d {dataset} -p {file_path} --unzip"
        try:
            context.log.info(f"Downloading dataset to {file_path}...")
            subprocess.run(command, shell=True, check=True)
            context.log.info("Dataset downloaded and unzipped successfully.")
        except subprocess.CalledProcessError as e:
            context.log.error(f"An error occurred while downloading the dataset: {e}")
            raise
    else:
        context.log.info("Dataset already exists. Skipping download.")

    return file_path
