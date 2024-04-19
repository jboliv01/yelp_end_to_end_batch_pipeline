with
    reviews as (
        select
            business_id,
            date_trunc('month', review_date) as review_month,  -- Grouping by month
            count(review_id) as total_reviews,
            avg(stars) as avg_stars
        from {{ ref('stg_yelp_reviews') }}
        group by business_id, date_trunc('month', review_date)  
    ),
    business as (
        select *
        from {{ ref('stg_yelp_business') }}
    ),
    reviews_by_month as (
        select
            bus.business_id,
            bus.business_name,
            bus.city,
            bus.state,
            bus.latitude,
            bus.longitude,
            rev.total_reviews,
            rev.avg_stars,
            rev.review_month
        from reviews rev
        left join business bus on rev.business_id = bus.business_id
    )
select *
from reviews_by_month