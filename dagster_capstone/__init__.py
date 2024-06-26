# fmt: off
from pathlib import Path

from dagster import Definitions, load_assets_from_modules, PipesSubprocessClient
from dagster_aws.s3 import S3Resource
# from dagster_aws.emr import emr_pyspark_step_launcher
# from dagster_pyspark import PySparkResource

from .assets import kaggle, spark, yelp, dbt, trips
from .resources import database_resource, dbt_resource 
from .jobs import yelp_etl_job
from .schedules import yelp_update_schedule

trip_assets = load_assets_from_modules([trips])
kaggle_assets = load_assets_from_modules([kaggle])
spark_assets = load_assets_from_modules([spark])
yelp_asssets = load_assets_from_modules([yelp])
dbt_analytics_assets = load_assets_from_modules(modules=[dbt]) 
# metric_assets = load_assets_from_modules([metrics])
# request_assets = load_assets_from_modules([requests])

all_jobs = [yelp_etl_job]
all_schedules = [yelp_update_schedule]
# all_sensors = [adhoc_request_sensor]

defs = Definitions(
    assets=[*kaggle_assets, *spark_assets, *yelp_asssets, *dbt_analytics_assets],
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
        "s3": S3Resource(region_name='us-east-2'),
        "kaggle_io_manager": kaggle.kaggle_file_manager.configured({
            "s3_bucket": "de-capstone-project",
            "s3_key_prefix": "yelp/raw/"
        }),
        "database": database_resource,
        "dbt": dbt_resource
        #"io_manager": ParquetIOManager(pyspark=emr_pyspark, path_prefix="s3://de-capstone-project/production"),
    },
    jobs=all_jobs,
    schedules=all_schedules,
)


# emr_pyspark = PySparkResource(spark_config={"spark.executor.memory": "2g"})

# defs = Definitions(
#     assets=[*kaggle_assets, *spark_assets, *yelp_asssets],
#     resources={
#         "pyspark_step_launcher": emr_pyspark_step_launcher.configured(
#             {
#                 "cluster_id": {"env": "EMR_CLUSTER_ID"},
#                 "local_job_package_path": str(Path(__file__).parent.parent),
#                 "deploy_local_job_package": True,
#                 "region_name": "us-west-2",
#                 "staging_bucket": "de-capstone-project",
#                 "staging_prefix": "staging",
#                 "wait_for_logs": True,
#             }
#         ),
#         "pyspark": emr_pyspark,
#         "s3": S3Resource(region_name='us-east-2'),
#         "kaggle_io_manager": kaggle.kaggle_file_manager.configured({
#             "s3_bucket": "de-capstone-project",
#             "s3_key_prefix": "yelp/raw/"
#         }),
#         #"io_manager": ParquetIOManager(pyspark=emr_pyspark, path_prefix="s3://de-capstone-project/production"),
#     },
# )

