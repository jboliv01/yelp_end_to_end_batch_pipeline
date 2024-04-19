import shutil

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    Definitions,
    PipesSubprocessClient,
    asset,
    file_relative_path,
    Field,
    AssetKey

)


@asset(config_schema={"region": Field(str, default_value="us-east-2", is_required=False)}, compute_kind='spark')
def create_emr_cluster(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ):
    cmd = [
        shutil.which("python"),
        file_relative_path(__file__, "external_create_emr_cluster.py"),
    ]

    region = context.op_config["region"]

    result = pipes_subprocess_client.run(
        command=cmd, context=context, extras={"region": region}
    ).get_materialize_result()

    return result

@asset(config_schema={"job_name": Field(str, default_value="YelpReviews"),
        "s3_spark_code_path": Field(str, default_value="s3://de-capstone-project/emr-resources/spark-code/emr_spark_yelp_reviews.py"),
        "region": Field(str, default_value="us-east-2"),
      },
    compute_kind='spark',
    deps=['create_emr_cluster']
)
def emr_pyspark_submit(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
    ):

    instance = context.instance
    materialization = instance.get_latest_materialization_event(AssetKey(["create_emr_cluster"])).asset_materialization
    
    cluster_id = materialization.metadata["cluster_id"].value
    job_name = context.op_config["job_name"]
    s3_spark_code_path = context.op_config["s3_spark_code_path"]
    region = context.op_config["region"]

    context.log.info(f"Cluster: {cluster_id}")
    context.log.info(f"Job Name: {job_name}")
    context.log.info(f"S3 Path: {s3_spark_code_path}")
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
            "s3_spark_code_path": s3_spark_code_path,
            "region": region,
        },
    )

    return result.get_materialize_result()


defs = Definitions(
    assets=[create_emr_cluster, emr_pyspark_submit],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)


