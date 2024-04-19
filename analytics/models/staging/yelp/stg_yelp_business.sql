with raw_bus as (
    select *
    from {{ source('raw_yelp', 'external_yelp_business') }}
)
select
    business_id,
    name as business_name,
    city,
    state,
    postal_code,
    latitude,
    longitude,
    review_count,
    stars,
    is_open,
    categories
from raw_bus