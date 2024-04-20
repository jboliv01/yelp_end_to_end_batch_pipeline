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


from dagster import asset, multi_asset, AssetOut, Output, Field
import polars as pl
import s3fs

from dagster_duckdb import DuckDBResource
from smart_open import open
from . import constants

# @multi_asset(
#     outs={
#         "yelp_user_data": AssetOut(is_required=False),
#         "yelp_business_data": AssetOut(is_required=False),
#         "yelp_tip_data": AssetOut(is_required=False),
#     },
#     can_subset=True,
#     config_schema={
#         "file_keys": Field(
#             dict,
#             default_value={
#                 "yelp_business_data": "yelp_academic_dataset_business.json",
#                 "yelp_user_data": "yelp_academic_dataset_user.json",
#                 "yelp_tip_data": "yelp_academic_dataset_tip.json",
#             },
#             is_required=False,
#         )
#     },
#     required_resource_keys={"s3"},
#     deps=["kaggle_file"],
#     group_name="extract",
#     compute_kind="python",
# )
# def yelp_data(context):
#     """load each yelp json dataset into a polars dataframe"""
#     s3 = context.resources.s3
#     s3_bucket = "de-capstone-project"
#     s3_prefix = "yelp/raw"
#     file_keys = context.op_config["file_keys"]

#     for asset_name, file_key in file_keys.items():
#         s3_path = f"{s3_prefix}/{file_key}"
#         context.log.info(f"s3 path: {s3_path}")
#         obj = s3.get_object(Bucket=s3_bucket, Key=s3_path)
#         context.log.info("reading json body")
#         content = obj["Body"].read()
#         context.log.info("loading lazy frame")
#         lazy_df = pl.read_ndjson(content).lazy()
#         context.log.info("deleting object and content")
#         del obj, content
#         context.log.info("yielding output")
#         yield Output(lazy_df, asset_name)


@asset(
    deps=['kaggle_file'],
    group_name="ingested",
    compute_kind="DuckDB",
)
def external_yelp_business(context, database: DuckDBResource):
    """The raw taxi zones dataset, loaded into a DuckDB database."""
    context.log.info(f'Reading raw data from: {constants.YELP_BUSINESS_DATA_FILE_PATH}')
    query = f"""
        create or replace table external_yelp_business as (
            select
                business_id,
                name,
                city,
                state,
                postal_code,
                latitude,
                longitude,
                review_count,
                stars,
                is_open,
                categories
            from read_json_auto('{constants.YELP_BUSINESS_DATA_FILE_PATH}', format = 'newline_delimited')
        );
    """

    if "md:?" in database.database:
        context.log.info(f"Writing to Motherduck Prod")
    else:
        context.log.info(f"Writing to local {database.database}")

    with database.get_connection() as conn:
        conn.execute(query)


@asset(
    deps=['kaggle_file'],
    group_name="ingested",
    compute_kind="DuckDB",
)
def external_yelp_users(context, database: DuckDBResource):
    """The raw taxi zones dataset, loaded into a DuckDB database."""
    context.log.info(f'Reading raw data from: {constants.YELP_USERS_DATA_FILE_PATH}')
    query = f"""
        create or replace table external_yelp_users as (
            select user_id,
            name,
            review_count,
            yelping_since
            from read_json_auto('{constants.YELP_USERS_DATA_FILE_PATH}', format = 'newline_delimited')
        );
    """

    if "md:?" in database.database:
        context.log.info(f"Writing to Motherduck Prod")
    else:
        context.log.info(f"Writing to local {database.database}")

    with database.get_connection() as conn:
        conn.execute(query)

@asset(
    deps=['partition_yelp_reviews'],
    group_name="ingested",
    compute_kind="DuckDB",
)
def external_yelp_reviews(context, database: DuckDBResource):
    """The raw taxi zones dataset, loaded into a DuckDB database."""
    context.log.info(f'Reading raw data from: {constants.YELP_REVIEWS_DATA_FILE_PATH}')
    query = f"""
        create or replace table external_yelp_reviews as (
            select business_id,
            review_id,
            user_id,
            stars,
            date,
            datetime,
            text
            from READ_PARQUET('{constants.YELP_REVIEWS_DATA_FILE_PATH}')
        );
    """

    if "md:?" in database.database:
        context.log.info(f"Writing to Motherduck Prod")
    else:
        context.log.info(f"Writing to local {database.database}")

    with database.get_connection() as conn:
        conn.execute(query)

# @asset(group_name="transform_ingest", compute_kind="polars")
# def yelp_users(context, yelp_user_data):
#     """returns a subset of yelp user data"""

#     df = yelp_user_data.collect()

#     fs = s3fs.S3FileSystem()

#     s3_bucket = "de-capstone-project"
#     s3_prefix = "yelp/processed/users"
#     s3_path = f"{s3_bucket}/{s3_prefix}/users.parquet"
#     context.log.info(f"s3 Export Path {s3_path}")

#     with fs.open(f"s3://{s3_path}", mode="wb") as f:
#         df.write_parquet(f, use_pyarrow=True, compression='snappy')

#     pass


# @asset(group_name="transform_ingest", compute_kind="polars")
# def yelp_businesses(context, yelp_business_data):
#     """returns a subset of yelp business data"""

#     df = yelp_business_data.collect()

#     df = df.select(
#         [
#             "business_id",
#             "name",
#             "address",
#             "city",
#             "state",
#             "postal_code",
#             "latitude",
#             "longitude",
#             "stars",
#             "review_count",
#             "is_open",
#             "categories",
#         ]
#     )

#     fs = s3fs.S3FileSystem()

#     s3_bucket = "de-capstone-project"
#     s3_prefix = "yelp/processed/businesses"
#     s3_path = f"{s3_bucket}/{s3_prefix}/file.parquet"
#     context.log.info(f"s3 Export Path {s3_path}")

#     with fs.open(f"s3://{s3_path}", mode="wb") as f:
#         df.write_parquet(f, use_pyarrow=True, compression='snappy')



# @asset(group_name="transform_ingest", compute_kind="polars")
# def yelp_tips(context, yelp_tip_data):
#     """returns a subset of yelp business data"""

#     df = yelp_tip_data.collect()

#     fs = s3fs.S3FileSystem()

#     s3_bucket = "de-capstone-project"
#     s3_prefix = "yelp/processed/tips"
#     s3_path = f"{s3_bucket}/{s3_prefix}/tips.parquet"
#     context.log.info(f"s3 Export Path {s3_path}")

#     with fs.open(f"s3://{s3_path}", mode="wb") as f:
#         df.write_parquet(f, use_pyarrow=True, compression='snappy')

#     pass

