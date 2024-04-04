from dagster import asset, multi_asset, AssetOut, Output, Field
import polars as pl


@multi_asset(
       outs={"yelp_user_data": AssetOut(), 
             "yelp_business_data": AssetOut(),
             "yelp_review_data": AssetOut(),
             "yelp_tip_data": AssetOut()
             },
       config_schema={'file_keys': Field(dict, is_required=True)},
       required_resource_keys={"s3"},
       deps=['kaggle_file'],
       group_name='yelp_assets',
       compute_kind='python')
def yelp_data(context) -> pl.DataFrame:
    '''load each yelp json dataset into a polars dataframe'''
    s3 = context.resources.s3
    s3_bucket = 'de-capstone-project'
    s3_prefix = 'yelp/raw'
    file_keys = context.op_config['file_keys']

    assets = {}
    for asset_name, file_key in file_keys.items():
        s3_path = f"{s3_prefix}/{file_key}"
        context.log.info(f's3 path: {s3_path}')
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_path)
        content = obj['Body'].read()
        lazy_df = pl.read_ndjson(content).lazy()
        assets[asset_name] = lazy_df

    for asset_name, df in assets.items():
        yield Output(df, asset_name)


@asset(group_name='yelp_assets',
       compute_kind='polars')
def yelp_users(yelp_user_data) -> pl.DataFrame:
    '''returns a subset of yelp user data'''
    return yelp_user_data.head(10).collect()

@asset(group_name='yelp_assets',
       compute_kind='polars')
def yelp_businesses(yelp_business_data) -> pl.DataFrame:
    '''returns a subset of yelp business data'''
    return yelp_business_data.head(10).collect()

@asset(group_name='yelp_assets',
       compute_kind='polars')
def yelp_reviews(yelp_review_data) -> pl.DataFrame:
    '''returns a subset of yelp business data'''
    return yelp_review_data.head(10).collect()

@asset(group_name='yelp_assets',
       compute_kind='polars')
def yelp_tips(yelp_tip_data) -> pl.DataFrame:
    '''returns a subset of yelp business data'''
    return yelp_tip_data.head(10).collect()

# @asset(config_schema={'file_key': Field(str)},
#        required_resource_keys={"s3"},
#        group_name='yelp_assets')
# def yelp_businesses_old(context) -> pl.DataFrame:
#     s3 = context.resources.s3
#     file_key = context.op_config['file_key']
#     s3_bucket = 'de-capstone-project'
#     s3_prefix = 'yelp/raw'

#     s3_path = f"{s3_prefix}/{file_key}"
#     context.log.info(f's3 path: {s3_path}')
#     # Download the file from S3
#     obj = s3.get_object(Bucket=s3_bucket, Key=s3_path)

#     # Read the content as a string
#     content = obj['Body'].read()

#     lazy_df = pl.read_ndjson(content).lazy() 

#     return lazy_df.head(10).collect()

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
