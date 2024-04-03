import io
from dagster import asset, IOManager, io_manager, Field
import polars as pl


class JsonToParquetS3IOManager(IOManager):
    def __init__(self, s3_resource, s3_bucket, s3_prefix):
        self.s3 = s3_resource
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def handle_output(self, context, obj: pl.DataFrame):
        buffer = io.BytesIO()
        pl.write_parquet(obj, buffer)
        buffer.seek(0)
        s3_path = f"{self.s3_prefix}/{context.asset_key.path[-1]}.parquet"
        self.s3.put_object(Bucket=self.s3_bucket, Key=s3_path, Body=buffer.getvalue())

    def load_input(self, context):
        context.log.info(f'op_config: {dir(context.op_config)}')
        context.log.info(f'keys: {context.op_config.keys}')
        file_key = context.resource_config['file_key']
        s3_path = f"{self.s3_prefix}/{file_key}"
        obj = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_path)
        buffer = io.BytesIO(obj['Body'].read())
        df = pl.read_json(buffer)
        return df

@io_manager(config_schema={
    "s3_bucket": str,
    "s3_prefix": str
    },
    required_resource_keys={"s3"})
def json_to_parquet_s3_io_manager(context):
    s3 = context.resources.s3
    s3_bucket = context.resource_config["s3_bucket"]
    s3_prefix = context.resource_config["s3_prefix"]
    return JsonToParquetS3IOManager(s3_resource=s3, s3_bucket=s3_bucket, s3_prefix=s3_prefix)

# Asset for processing the data
@asset(
    config_schema={'file_key': Field(str)},
    io_manager_key='json_to_parquet_s3_io_manager'
)
def yelp_users(context) -> pl.DataFrame:
    context.log.info(f'context: {dir(context)}')
    df = context.resources.json_to_parquet_s3_io_manager.load_input(context)
    # Process the data
    return df.head(1000)


@asset(config_schema={'file_key': Field(str)},
       required_resource_keys={"s3"})
def yelp_users_test(context) -> pl.DataFrame:
    s3 = context.resources.s3
    file_key = context.op_config['file_key']
    s3_bucket = 'de-capstone-project'
    s3_prefix = 'yelp/raw'

    s3_path = f"{s3_prefix}/{file_key}"
    context.log.info(f's3 path: {s3_path}')
    # Download the file from S3
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_path)
    bytes_to_read = 1024  # for example, read the first 1024 bytes
    head = obj['Body'].read(bytes_to_read)
    context.log.info(f'head: {head}')

    # Read the content as a string
    content = obj['Body'].read().decode('utf-8')
    
    # Use StringIO to provide a file-like object to Polars
    with io.StringIO(content) as f:
        lazy_df = pl.read_ndjson(f)

    return lazy_df.head(1000).collect()

# @asset(
#     config_schema={'file_key': Field(str)},
#     io_manager_key='json_to_parquet_s3_io_manager',
#     required_resource_keys={"s3"},
#     compute_kind='polars'
#     )
# def yelp_businesses(raw_data: pl.DataFrame) -> pl.DataFrame:
#     processed_data = raw_data.head(1000)
#     return processed_data


# @asset(
#     required_resource_keys={"s3"},
#     deps=['kaggle_file'],
#     compute_kind='polars'
# )
# def yelp_users(context) -> pl.DataFrame:
#     s3_bucket = 'de-capstone-project'
#     file_key = 'yelp/raw/yelp_academic_dataset_user.json'

#     # Use the S3 client from S3Resource to get the file object
#     s3 = context.resources.s3
#     s3_obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
#     context.log.info(f'S3 Object: {s3_obj}')

#     body = s3_obj['Body'].read()

#     with io.BytesIO(body) as f:
#         df = pl.read_ndjson(f)

#     # Return the first 1000 rows
#     return df.head(1000)

# @asset(
#     required_resource_keys={"s3"},
#     deps=['kaggle_file'],
#     compute_kind='polars'
# )
# def yelp_reviews(context) -> pl.DataFrame:
#     s3_bucket = 'de-capstone-project'
#     file_key = 'yelp/raw/yelp_academic_dataset_review.json'

#     # Use the S3 client from S3Resource to get the file object
#     s3 = context.resources.s3
#     s3_obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
#     context.log.info(f'S3 Object: {s3_obj}')

#     body = s3_obj['Body'].read()

#     with io.BytesIO(body) as f:
#         df = pl.read_ndjson(f)

#     # Return the first 1000 rows
#     return df.head(1000)

# @asset(
#     required_resource_keys={"s3"},
#     deps=['kaggle_file'],
#     compute_kind='polars'
# )
# def yelp_tips(context) -> pl.DataFrame:
#     s3_bucket = 'de-capstone-project'
#     file_key = 'yelp/raw/yelp_academic_dataset_tip.json'

#     # Use the S3 client from S3Resource to get the file object
#     s3 = context.resources.s3
#     s3_obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
#     context.log.info(f'S3 Object: {s3_obj}')

#     body = s3_obj['Body'].read()

#     with io.BytesIO(body) as f:
#         df = pl.read_ndjson(f)

#     # Return the first 1000 rows
#     return df.head(1000)


