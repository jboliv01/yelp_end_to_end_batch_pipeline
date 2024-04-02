from dagster import asset, AssetIn
import polars as pl
import os

@asset(
    io_manager_key="polars_parquet_io_manager",
    required_resource_keys={"s3"},
    ins={'kaggle_file': AssetIn()}
)
def json_to_parquet(context, kaggle_file: str) -> pl.DataFrame:

    json_filename = "specific_file.json"
    json_file_path = os.path.join(kaggle_file, json_filename)
    context.log.info(f"json path {json_file_path}")

    # Read the JSON file into a Polars DataFrame
    df = pl.read_json(json_file_path)

    # Define the S3 path for the output Parquet file
    s3_bucket = "de-capstone-project"
    s3_key = f"pipeline-output/{os.path.basename(json_filename).replace('.json', '.parquet')}"
    context.log.info(f"Writing Parquet to: s3://{s3_bucket}/{s3_key}")

    # Write the DataFrame to S3 in Parquet format
    s3_path = f"s3://{s3_bucket}/{s3_key}"
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
