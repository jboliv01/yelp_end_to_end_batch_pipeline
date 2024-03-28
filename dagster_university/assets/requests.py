from dagster import asset, Config
from dagster_duckdb import DuckDBResource

import plotly.express as px
import plotly.io as pio

from . import constants

class AdhocRequestConfig(Config):
    filename: str
    borough: str
    start_date: str
    end_date: str

@asset(
    deps=['taxi_zones', 'taxi_trips']
)
def adhoc_request(config: AdhocRequestConfig, database: DuckDBResource):
    file_path = constants.REQUEST_DESTINATION_TEMPLATE_FILE_PATH.format(config.filename.split('.')[0])
    query = f"""
        select
            date_part('hour', pickup_datetime) as hour_of_day,
            date_part('dayofweek', pickup_datetime) as day_of_week_num,
            case date_part('dayofweek', pickup_datetime)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            end as day_of_week,
            count(*) as num_trips
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where pickup_datetime >= '{config.start_date}'
        and pickup_datetime < '{config.end_date}'
        and pickup_zone_id in (
            select zone_id
            from zones
            where borough = '{config.borough}'
        )
        group by 1, 2
        order by 1, 2 asc
        """
    
    with database.get_connection() as conn:
        results = conn.execute(query).fetch_df()

    fig = px.bar(
        results,
        x="hour_of_day",
        y="num_trips",
        color="day_of_week",
        barmode="stack",
        title=f"Number of trips by hour of day in {config.borough}, from {config.start_date} to {config.end_date}",
        labels={
            "hour_of_day": "Hour of Day",
            "day_of_week": "Day of Week",
            "num_trips": "Number of Trips"
        }
    )

    pio.write_image(fig, file_path)
