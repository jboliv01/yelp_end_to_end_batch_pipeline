import shutil

from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    PipesSubprocessClient,
    asset,
    file_relative_path,
    Field
)


@asset
def subprocess_asset(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:
    cmd = [shutil.which("python"), file_relative_path(__file__, "external_code.py")]
    return pipes_subprocess_client.run(
        command=cmd, context=context
    ).get_materialize_result()


@asset(
    config_schema={'cluster_id': Field(str, default_value='j-2YLAXTLFQWW5Z', is_required=True),
                   'job_name': Field(str, default_value='YelpReviews', is_required=True),
                   's3_spark_code_path': Field(str, default_value='s3://de-capstone-project/emr-resources/spark-code/emr_spark_yelp_reviews.py', is_required=True),
                   'region': Field(str, default_value='us-west-2', is_required=True)
    }
)
def emr_pyspark_submit(
    context: AssetExecutionContext, pipes_subprocess_client: PipesSubprocessClient
) -> MaterializeResult:

    cluster_id = context.op_config['cluster_id']
    job_name = context.op_config['job_name']
    s3_spark_code_path = context.op_config['s3_spark_code_path']
    region = context.op_config['region']

    cmd = [
        shutil.which("python"), 
        file_relative_path(__file__, "external_spark.py"),
        cluster_id,
        job_name,
        s3_spark_code_path,
        region  
        ]
    
    return pipes_subprocess_client.run(
        command=cmd, 
        context=context
    ).get_materialize_result()

defs = Definitions(
    assets=[subprocess_asset],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)

