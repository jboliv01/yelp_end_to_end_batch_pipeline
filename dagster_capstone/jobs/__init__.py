from dagster import AssetSelection, define_asset_job
from dagster_dbt import build_dbt_asset_selection

from ..partitions import monthly_partition, weekly_partition
from ..assets.dbt import dbt_analytics

trips_by_week = AssetSelection.keys("trips_by_week")
adhoc_request = AssetSelection.keys("adhoc_request")

dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips").downstream()

yelp_etl_job = define_asset_job(
    name="yelp_etl_job",
    partitions_def=monthly_partition,
    selection=AssetSelection.all(),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 2,  # limits concurrent assets to 2
                },
            }
        }
    }
)



# weekly_update_job = define_asset_job(
#     name="weekly_update_job",
#     partitions_def=weekly_partition,
#     selection=trips_by_week
# )

# adhoc_request_job = define_asset_job(
#     name="adhoc_request_job",
#     selection=adhoc_request,
# )
