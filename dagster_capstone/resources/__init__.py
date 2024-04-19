import os
import boto3

from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

from ..assets.constants import DBT_DIRECTORY

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)

dbt_resource = DbtCliResource(
    project_dir=DBT_DIRECTORY,
)

if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    database_resource = DuckDBResource(
        database=EnvVar("MOTHERDUCK_DATABASE"),
        connection_config={"motherduck_token": EnvVar("MOTHERDUCK_TOKEN")},
    )
    session = boto3.Session(
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        region_name=EnvVar("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}
    database_resource = DuckDBResource(
        database=EnvVar("DUCKDB_DATABASE"),
    )
