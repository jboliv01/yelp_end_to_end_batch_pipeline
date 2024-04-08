import shutil

from dagster import (
    AssetExecutionContext,
    Definitions,
    PipesSubprocessClient,
    asset,
    file_relative_path,
    Field
)


@asset(config_schema={"region": Field(str, default_value="us-west-2", is_required=False)})
def create_emr_cluster(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
    ):
    cmd = [
        shutil.which("python"),
        file_relative_path(__file__, "create_emr_cluster.py"),
    ]

    region = context.op_config["region"]

    result = pipes_subprocess_client.run(
        command=cmd, context=context, extras={"region": region}
    ).get_materialize_result()

    return result


@asset(config_schema={"job_name": Field(str, default_value="YelpReviews"),
        "s3_spark_code_path": Field(str, default_value="s3://de-capstone-project/emr-resources/spark-code/emr_spark_yelp_reviews.py"),
        "region": Field(str, default_value="us-west-2")})
def emr_pyspark_submit(
    create_emr_cluster: str,
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
    ):

    cluster_id = create_emr_cluster
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
        file_relative_path(__file__, "external_spark.py"),
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

@asset
def test():
    pass


defs = Definitions(
    assets=[create_emr_cluster, emr_pyspark_submit],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)
