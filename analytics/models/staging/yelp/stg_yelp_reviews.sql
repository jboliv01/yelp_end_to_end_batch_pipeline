with raw_reviews as (
    select *
    from {{ source('raw_yelp', 'external_yelp_reviews') }}
)
select 
    business_id,
    review_id,
    user_id,
    stars,
    datetime,
    text
from raw_reviews