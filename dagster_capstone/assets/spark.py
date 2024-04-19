import shutil
import os

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    Definitions,
    PipesSubprocessClient,
    asset,
    file_relative_path,
    Field,
    AssetKey,
    EnvVar
)

from ..resources import session

@asset(
    config_schema={"region": Field(str, default_value="us-east-2", is_required=False), 
                   "s3_bucket_prefix": Field(str, default_value="s3://de-capstone-project/", is_required=False),
                   "vpc_default_subnet_id": Field(str, default_value="subnet-6762990c")
                   },
    compute_kind="spark",
    group_name="compute",
    deps=["kaggle_file"],
)
def emr_cluster(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
):
    cmd = [
        shutil.which("python"),
        file_relative_path(__file__, "external_create_emr_cluster.py"),
    ]

    
    s3_bucket_prefix = context.op_config["s3_bucket_prefix"]
    vpc_default_subnet_id = context.op_config["vpc_default_subnet_id"]
    region = context.op_config["region"]
    # overwrite test
    s3_bucket_prefix = os.getenv("S3_BUCKET_PREFIX")
    vpc_default_subnet_id = os.getenv("VPC_DEFAULT_SUBNET_ID")
    region = os.getenv("AWS_REGION")
    

    context.log.info(f"EMR Region: {region}")
    context.log.info(f"S3 Bucket Path: {s3_bucket_prefix}")
    context.log.info(f"Default VPC Subnet ID: {vpc_default_subnet_id}")
    

    result = pipes_subprocess_client.run(
        command=cmd, context=context, extras={"region": region, "s3_bucket_prefix": s3_bucket_prefix, "vpc_default_subnet_id": vpc_default_subnet_id}
    ).get_materialize_result()

    return result


@asset(
    config_schema={
        "s3_spark_code_path": Field(
            str,
            default_value="emr-resources/spark-code/emr_spark_yelp_reviews.py",
        ),
        "s3_bucket_prefix": Field(str, default_value='s3://de-capstone-project/', is_required=False),
        "region": Field(str, default_value="us-east-2"),
    },
    compute_kind="spark",
    group_name="ingested",
    deps=["emr_cluster"],
)
def partition_yelp_reviews(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
):

    instance = context.instance
    materialization = instance.get_latest_materialization_event(
        AssetKey(["emr_cluster"])
    ).asset_materialization

    cluster_id = materialization.metadata["cluster_id"].value
    job_name = "YelpReviews"

    s3_bucket_prefix = context.op_config['s3_bucket_prefix']
    s3_spark_code_prefix = f'{s3_bucket_prefix}{context.op_config["s3_spark_code_prefix"]}'
    region = context.op_config["region"]

    s3_bucket_prefix = os.getenv("S3_BUCKET_PREFIX")
    s3_spark_code_prefix = f'{s3_bucket_prefix}{os.getenv("S3_SPARK_CODE_PREFIX")}'
    region = os.getenv("AWS_REGION")
    

    context.log.info(f"Cluster: {cluster_id}")
    context.log.info(f"Job Name: {job_name}")
    context.log.info(f"S3 Path: {s3_spark_code_prefix}")
    context.log.info(f"EMR Region: {region}")

    python_executable = shutil.which("python")
    if not python_executable:
        raise EnvironmentError("Python executable not found.")

    cmd = [
        python_executable,
        file_relative_path(__file__, "external_run_spark_job.py"),
    ]

    context.log.info(f"command: {cmd}")

    result = pipes_subprocess_client.run(
        command=cmd,
        context=context,
        extras={
            "cluster_id": cluster_id,
            "job_name": job_name,
            "s3_spark_code_path": s3_spark_code_prefix,
            "region": region,
        },
    )

    return result.get_materialize_result()


# defs = Definitions(
#     assets=[create_emr_cluster, emr_pyspark_submit],
#     resources={"pipes_subprocess_client": PipesSubprocessClient()},
# )


# The code below enables pyspark code to defined within dagster, but from my understanding, will require
# a hybrid agent deployment to enable pyspark to be executed within dagster

# from pathlib import Path
# from typing import Any

# from dagster import ConfigurableIOManager, ResourceParam, asset, AssetIn
# from dagster_pyspark import PySparkResource
# from pyspark.sql import DataFrame, Row
# from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# class ParquetIOManager(ConfigurableIOManager):
#     pyspark: PySparkResource
#     path_prefix: str
#     def _get_path(self, context) -> str:
#         return "/".join([context.resource_config["path_prefix"], *context.asset_key.path])
#     def handle_output(self, context, obj):
#         obj.write.parquet(self._get_path(context))
#     def load_input(self, context):
#         spark = self.pyspark.spark_session
#         return spark.read.parquet(self._get_path(context.upstream_output))

# @asset(compute_kind='spark')
# def people(pyspark: PySparkResource, pyspark_step_launcher: ResourceParam[Any]) -> DataFrame:
#     schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
#     rows = [Row(name="Thom", age=51), Row(name="Jonny", age=48), Row(name="Nigel", age=49)]
#     pyspark.spark_session.createDataFrame(rows, schema)

#     pass

# @asset(compute_kind='spark')
# def people_over_50(pyspark_step_launcher: ResourceParam[Any], people: DataFrame) -> DataFrame:
#     return people.filter(people["age"] > 50)
