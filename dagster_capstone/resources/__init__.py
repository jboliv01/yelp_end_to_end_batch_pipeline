import os
import boto3

from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

from ..assets.constants import DBT_DIRECTORY

dbt_resource = DbtCliResource(
    project_dir=DBT_DIRECTORY,
)

if os.getenv("DAGSTER_ENVIRONMENT", "dev") == "prod":
    database_resource = DuckDBResource(
        database=os.getenv("MOTHERDUCK_DATABASE"),
        connection_config={"motherduck_token": os.getenv("MOTHERDUCK_TOKEN")},
    )
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}
    database_resource = DuckDBResource(
        database=EnvVar("DUCKDB_DATABASE"),
    )
