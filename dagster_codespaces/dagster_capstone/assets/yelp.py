import boto3
from dagster import asset, AssetIn, Out, Field
import pandas as pd
import polars as pl


@asset(
    required_resource_keys={'s3'},
    config_schema={'s3_bucket': Field(str), 's3_key': Field(str)},
    ins={'kaggle_file': AssetIn()},
    outs={'processed_data': Out()}
)
def yelp_businesses(context) -> pl.DataFrame:
    s3_client = context.resources.s3
    s3_bucket = context.op_config['s3_bucket']
    s3_key = context.op_config['s3_key'] + 'yelp_academic_dataset_business.json'

    # Get the object from S3
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    
    # Read the file into a Polars dataframe
    df = pl.read_json(obj['Body'])

    return df

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
