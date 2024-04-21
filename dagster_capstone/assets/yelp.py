
from dagster import asset, multi_asset, AssetOut, Output, Field
import polars as pl
import s3fs

from dagster_duckdb import DuckDBResource
from smart_open import open
from . import constants

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

