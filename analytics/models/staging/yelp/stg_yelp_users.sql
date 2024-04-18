with raw_users as (
    select *
    from {{ source('raw_yelp', 'external_yelp_users') }}
)
select
    user_id,
    name,
    review_count,
    yelping_since
from raw_users