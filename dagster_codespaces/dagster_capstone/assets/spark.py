from pathlib import Path
from typing import Any

from dagster import ConfigurableIOManager, ResourceParam, asset, AssetIn
from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

class ParquetIOManager(ConfigurableIOManager):
    pyspark: PySparkResource
    path_prefix: str
    def _get_path(self, context) -> str:
        return "/".join([context.resource_config["path_prefix"], *context.asset_key.path])
    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))
    def load_input(self, context):
        spark = self.pyspark.spark_session
        return spark.read.parquet(self._get_path(context.upstream_output))

@asset(compute_kind='spark')
def people(pyspark: PySparkResource, pyspark_step_launcher: ResourceParam[Any]) -> DataFrame:
    schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
    rows = [Row(name="Thom", age=51), Row(name="Jonny", age=48), Row(name="Nigel", age=49)]
    return pyspark.spark_session.createDataFrame(rows, schema)

@asset(compute_kind='spark')
def people_over_50(pyspark_step_launcher: ResourceParam[Any], people: DataFrame) -> DataFrame:
    return people.filter(people["age"] > 50)

