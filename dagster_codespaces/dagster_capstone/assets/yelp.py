from dagster import asset, AssetIn
import polars as pl
import os

@asset(
    io_manager_key="polars_parquet_io_manager",
    required_resource_keys={"s3"}
)
def json_to_parquet(context) -> pl.DataFrame:
    s3_bucket = "de-capstone-project"
    s3_key = "yelp/raw/"
    json_filename = "yelp_academic_dataset_business.json"
    s3_path = f"s3://{s3_bucket}/{s3_key}/{json_filename}"

    context.log.info(f"json path {s3_path}")

    # Read the JSON file into a Polars DataFrame
    df = pl.read_json(s3_path)

    # Define the S3 path for the output Parquet file
    s3_key = f"yelp/processed/"
    parquet_filename = json_filename.replace('.json', '.parquet')
    context.log.info(f"Writing Parquet to: s3://{s3_bucket}/{s3_key}/{parquet_filename}")

    # Write the DataFrame to S3 in Parquet format
    s3_path = f"s3://{s3_bucket}/{s3_key}/{parquet_filename}"
    df.write_parquet(s3_path)

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
