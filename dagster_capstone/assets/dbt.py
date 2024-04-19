import os
import json
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..partitions import daily_partition
from .constants import DBT_DIRECTORY
from ..resources import dbt_resource

INCREMENTAL_SELECTOR = "config.materialized:incremental"

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            source = dbt_resource_props["source_name"]
            if source  == 'raw_yelp':
                return AssetKey(f"{name}")
            else:
                return AssetKey(f"taxi_{name}")
        else:
            return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]


# dbt_resource.cli(["--quiet", "parse"]).wait()

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_manifest_path = (
        dbt_resource.cli(["--quiet", "parse"]).wait()
        .target_path.joinpath("manifest.json")
    )
else:
    dbt_manifest_path = os.path.join(DBT_DIRECTORY, "target", "manifest.json")

@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR,
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--target", "prod"], context=context).stream()
