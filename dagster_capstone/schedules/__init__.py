from dagster import ScheduleDefinition
from ..jobs import yelp_etl_job

# trip_update_schedule = ScheduleDefinition(
#     job=trip_update_job,
#     cron_schedule="0 0 5 * *", # every 5th of the month at midnight
# )

# weekly_update_schedule = ScheduleDefinition(
#     job=weekly_update_job,
#     cron_schedule="0 0 * * 1" # every Monday at midnight
# )

yelp_update_schedule = ScheduleDefinition(
    job=yelp_etl_job,
    cron_schedule="0 0 5 * *", # every 5th of the month at midnight
)