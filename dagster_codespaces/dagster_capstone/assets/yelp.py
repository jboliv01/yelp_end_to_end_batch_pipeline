import io
from dagster import asset, IOManager, io_manager, Field
import polars as pl


@asset(config_schema={'file_key': Field(str)},
       required_resource_keys={"s3"},
       group_name='yelp_assets',
       compute_kind='polars')
def yelp_data(context) -> pl.DataFrame:
    s3 = context.resources.s3
    file_key = context.op_config['file_key']
    s3_bucket = 'de-capstone-project'
    s3_prefix = 'yelp/raw'

    s3_path = f"{s3_prefix}/{file_key}"
    context.log.info(f's3 path: {s3_path}')
    # Download the file from S3
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_path)

    # Read the content as a string
    content = obj['Body'].read()

    lazy_df = pl.read_ndjson(content).lazy() 

    return lazy_df


yelp_user_data = yelp_data.configured({"file_key": "yelp_academic_dataset_user.json"}, name='yelp_user_data')
yelp_business_data = yelp_data.configured({"file_key": "yelp_academic_dataset_business.json"}, name='yelp_business_data')

@asset(group_name='yelp_assets')
def yelp_users(yelp_user_data):
    return yelp_user_data.head(10).collect()

@asset(group_name='yelp_assets')
def yelp_businesses(yelp_business_data):
    return yelp_business_data.head(10).collect()

@asset(config_schema={'file_key': Field(str)},
       required_resource_keys={"s3"},
       group_name='yelp_assets')
def yelp_businesses_old(context) -> pl.DataFrame:
    s3 = context.resources.s3
    file_key = context.op_config['file_key']
    s3_bucket = 'de-capstone-project'
    s3_prefix = 'yelp/raw'

    s3_path = f"{s3_prefix}/{file_key}"
    context.log.info(f's3 path: {s3_path}')
    # Download the file from S3
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_path)

    # Read the content as a string
    content = obj['Body'].read()

    lazy_df = pl.read_ndjson(content).lazy() 

    return lazy_df.head(10).collect()





# class JsonToParquetS3IOManager(IOManager):
#     def __init__(self, s3_resource, s3_bucket, s3_prefix):
#         self.s3 = s3_resource
#         self.s3_bucket = s3_bucket
#         self.s3_prefix = s3_prefix

#     def handle_output(self, context, obj: pl.DataFrame):
#         buffer = io.BytesIO()
#         pl.write_parquet(obj, buffer)
#         buffer.seek(0)
#         s3_path = f"{self.s3_prefix}/{context.asset_key.path[-1]}.parquet"
#         self.s3.put_object(Bucket=self.s3_bucket, Key=s3_path, Body=buffer.getvalue())

#     def load_input(self, context):
#         context.log.info(f'op_config: {dir(context.op_config)}')
#         context.log.info(f'keys: {context.op_config.keys}')
#         file_key = context.resource_config['file_key']
#         s3_path = f"{self.s3_prefix}/{file_key}"
#         obj = self.s3.get_object(Bucket=self.s3_bucket, Key=s3_path)
#         buffer = io.BytesIO(obj['Body'].read())
#         df = pl.read_json(buffer)
#         return df

# @io_manager(config_schema={
#     "s3_bucket": str,
#     "s3_prefix": str
#     },
#     required_resource_keys={"s3"})
# def json_to_parquet_s3_io_manager(context):
#     s3 = context.resources.s3
#     s3_bucket = context.resource_config["s3_bucket"]
#     s3_prefix = context.resource_config["s3_prefix"]
#     return JsonToParquetS3IOManager(s3_resource=s3, s3_bucket=s3_bucket, s3_prefix=s3_prefix)
