KAGGLE_DATASET='yelp-dataset/yelp-dataset'
KAGGLE_FILE_PATH=".data/raw/kaggle"

import os
from pathlib import Path


S3_BUCKET_PREFIX = os.getenv("S3_BUCKET_PREFIX", "s3://dagster-university/")


def get_path_for_env(path: str) -> str:
    """A utility method for Dagster University. Generates a path based on the environment.

    Args:
        path (str): The local path to the file.

    Returns:
        result_path (str): The path to the file, based on the environment.
    """
    if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
        return S3_BUCKET_PREFIX + path
    else:
        return path


YELP_BUSINESS_DATA_FILE_PATH = get_path_for_env(os.path.join("yelp", "raw", "yelp_academic_dataset_business.json"))
YELP_USERS_DATA_FILE_PATH = get_path_for_env(os.path.join("yelp", "raw", "yelp_academic_dataset_user.json"))
YELP_REVIEWS_DATA_FILE_PATH = get_path_for_env(os.path.join("yelp", "processed", "reviews" , "year=2021", "month=*", "*.parquet"))

TAXI_ZONES_FILE_PATH = get_path_for_env(os.path.join("data", "raw", "taxi_zones.csv"))
TAXI_TRIPS_TEMPLATE_FILE_PATH = get_path_for_env(
    os.path.join("data", "raw", "taxi_trips_{}.parquet")
)

TRIPS_BY_AIRPORT_FILE_PATH = get_path_for_env(
    os.path.join("data", "outputs", "trips_by_airport.csv")
)
TRIPS_BY_WEEK_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "trips_by_week.csv"))
MANHATTAN_STATS_FILE_PATH = get_path_for_env(
    os.path.join("data", "staging", "manhattan_stats.geojson")
)
MANHATTAN_MAP_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "manhattan_map.png"))

REQUEST_DESTINATION_TEMPLATE_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "{}.png"))

DATE_FORMAT = "%Y-%m-%d"

START_DATE = "2023-01-01"
END_DATE = "2023-04-01"

AIRPORT_TRIPS_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "airport_trips.png"))

DBT_DIRECTORY = Path(__file__).joinpath("..", "..", "..", "analytics").resolve()

AIRPORT_TRIPS_FILE_PATH = get_path_for_env(os.path.join("data", "outputs", "airport_trips.png"))