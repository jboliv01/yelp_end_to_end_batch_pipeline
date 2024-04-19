-- models/dim_business_categories.sql

with raw_bus as (
    select 
        business_id,
        categories
    from {{ source('raw_yelp', 'external_yelp_business') }}
),

expanded_categories as (
    select
        business_id,
        unnest(string_split(categories, ',')) as category  -- Using unnest to explode categories
    from raw_bus
)

select
    business_id,
    trim(both ' ' from category) as category  -- Trim spaces from category names
from expanded_categories
