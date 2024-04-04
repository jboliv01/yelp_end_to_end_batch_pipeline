from dagster import job, op, Field
import boto3
import polars as pl
import json

@op(required_resource_keys={"s3"})
def fetch_ndjson_from_s3(context, bucket_name, file_key):
    '''loads json object from s3'''
    s3 = context.resources.s3
    s3_path = f"{bucket_name}/{file_key}"
    obj = s3.get_object(Bucket=bucket_name, Key=s3_path)
    lines = obj['Body'].read().decode('utf-8').splitlines()
    return [json.loads(line) for line in lines]

@op
def transform_data(context, data):
    df = pl.DataFrame(data)
    # Perform transformation using Polars
    transformed_df = df # Placeholder for actual transformation logic
    return transformed_df

@op(required_resource_keys={"s3"})
def write_to_s3(context, bucket_name, file_key, data):
    s3 = context.resources.s3
    obj = s3.Object(bucket_name, file_key)
    # Assuming data is a Polars DataFrame
    ndjson_str = "\n".join(data.to_dicts(into='records'))
    obj.put(Body=ndjson_str)

@job
def ndjson_processing_pipeline():
    bucket_name = 'de-capstone-project'
    file_key = 'yelp_academic_dataset_business.json'
    fetch_data = fetch_ndjson_from_s3(bucket_name, file_key)
    transformed_data = transform_data(fetch_data)
    #write_to_s3(bucket_name, 'transformed-file-key.parquet', transformed_data)
