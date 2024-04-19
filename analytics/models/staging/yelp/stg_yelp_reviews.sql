with raw_reviews as (
	select
    	business_id,
    	review_id,
    	user_id,
    	stars,
    	date,
    	datetime,
    	text  -- Assuming you have a separate 'date' column handled elsewhere
	from {{ source('raw_yelp', 'external_yelp_reviews') }}
)

select
	cast(business_id as varchar) as business_id,
	cast(review_id as varchar) as review_id,  -- Already assumed to be in the correct format
	cast(user_id as varchar) as user_id,  -- Casting for consistency
	cast(stars as float) as stars,  -- Ensuring stars is an float
	cast(datetime as timestamp) as review_datetime,  -- Casting to timestamp if needed
	cast(date as date) as review_date,  -- Casting to date if needed
	cast(text as varchar) as review_text
from raw_reviews
