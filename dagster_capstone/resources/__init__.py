import os
import boto3

from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

from ..assets.constants import DBT_DIRECTORY

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE").get_value()
)

dbt_resource = DbtCliResource(
    project_dir=DBT_DIRECTORY,
)

if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    database_resource = DuckDBResource(
        database=EnvVar("MOTHERDUCK_DATABASE").get_value(),
        connection_config={"motherduck_token": EnvVar("MOTHERDUCK_TOKEN").get_value()},
    )
    session = boto3.Session(
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
        region_name=EnvVar("AWS_REGION").get_value(),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}
    database_resource = DuckDBResource(
        database=EnvVar("DUCKDB_DATABASE"),
    )
