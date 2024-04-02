import boto3
from dagster import asset, AssetIn, IOManager, io_manager, Field
import pandas as pd
import polars as pl


@asset(required_resource_keys={'s3'})
def yelp_businesses(context, kaggle_file: AssetIn) -> pd.DataFrame:
    s3_client = context.resources.s3
    bucket_name='de-capstone-project' 
    s3_key='yelp/raw/' 
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)

    file_path = f"{kaggle_file}yelp_academic_dataset_business.json"
    # Process the data and then write back to S3 or another location
    output_path = "s3://de-capstone-project/staging/yelp_businesses/"

    return 1

# @asset(
#     ins={'kaggle_file': AssetIn}
# )
# def yelp_users(pyspark_step_launcher: ResourceParam[Any]) -> DataFrame:
#     return 1

# @asset(
#     ins={'kaggle_file': AssetIn}
# )
# def yelp_reviews(pyspark_step_launcher: ResourceParam[Any]) -> DataFrame:
#     return 1

# @asset(
#     ins={'kaggle_file': AssetIn}
# )
# def yelp_ratings(pyspark_step_launcher: ResourceParam[Any]) -> DataFrame:
#     return 1
