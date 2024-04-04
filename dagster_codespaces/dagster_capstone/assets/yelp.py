from dagster import asset, multi_asset, AssetOut, Output, Field
import polars as pl
import s3fs

# @asset(required_resource_keys={"s3"},
#        group_name='yelp_assets')
# def write_to_s3(context):
    
#     s3 = context.resources.s3

#     df = pl.DataFrame({
#     "name": ["Alice", "Bob", "Charlie"]
#     })

#     context.log.info(f's3 credentials {dir(s3._get_credentials())}')

#     #s3_session = s3._get_credentials().get_frozen_credentials()
#     # context.log.info(f's3 credentials dir {dir(s3_session)}')
#     # context.log.info(f's3 credentials {s3_session}')

#     fs = s3fs.S3FileSystem()
#     s3_bucket = 'de-capstone-project'
#     s3_prefix = 'yelp/processed/test'
#     s3_path = f"{s3_bucket}/{s3_prefix}/file.parquet"
#     context.log.info(f's3 Export Path {s3_path}')

#     # Use s3fs to open a file in write mode and write the dataframe to it
#     with fs.open(f's3://{s3_path}', mode='wb') as f:
#         df.write_parquet(f)

#     return 'complete'


@multi_asset(
       outs={"yelp_user_data": AssetOut(is_required=False), 
             "yelp_business_data": AssetOut(is_required=False),
             "yelp_review_data": AssetOut(is_required=False),
             "yelp_tip_data": AssetOut(is_required=False)
             },
       can_subset=True,
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
        context.log.info(f'reading JSON body')
        content = obj['Body'].read()
        context.log.info(f'loading dataframe')
        lazy_df = pl.read_ndjson(content)
        assets[asset_name] = lazy_df

    for asset_name, df in assets.items():
        yield Output(df, asset_name)


# @asset(group_name='yelp_assets',
#        compute_kind='polars')
# def yelp_users(yelp_user_data) -> pl.DataFrame:
#     '''returns a subset of yelp user data'''
#     return yelp_user_data.head(10).collect()

# @asset(group_name='yelp_assets',
#        required_resource_keys={"s3"},
#        compute_kind='polars')
# def yelp_businesses(yelp_business_data) -> pl.DataFrame:
#     '''returns a subset of yelp business data'''
#     return 1

@asset(group_name='yelp_assets',
       required_resource_keys={"s3"},
       compute_kind='polars')
def yelp_reviews(context, yelp_review_data: pl.DataFrame):
    '''returns a subset of yelp business data'''
    df = yelp_review_data
    df = df.with_columns(pl.col('date').str.strptime(pl.Datetime).alias('datetime')).drop('date')
    df = df.with_columns([
        pl.col('datetime').dt.date().alias('date'),
        pl.col('datetime').dt.year().alias('year'),
        pl.col('datetime').dt.month().alias('month')
        ])
 
    fs = s3fs.S3FileSystem()
    s3_bucket = 'de-capstone-project'
    s3_prefix = 'yelp/processed/reviews'
    s3_path = f"{s3_bucket}/{s3_prefix}"
    context.log.info(f's3 Export Path {s3_path}')

    # Use s3fs to open a file in write mode and write the dataframe to it
    with fs.open(f's3://{s3_path}', mode='wb') as f:
        df.collect().write_parquet(s3_path, use_pyarrow=True, pyarrow_options={"partition_cols": ["year","month"]})

    return 'complete'

# @asset(group_name='yelp_assets',
#        compute_kind='polars')
# def yelp_tips(yelp_tip_data) -> pl.DataFrame:
#     '''returns a subset of yelp business data'''
#     return yelp_tip_data.head(10).collect()

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
