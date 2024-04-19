---
title: Data Engineering Zoomcamp Capstone
---

<Dropdown data={distinct_categories} name=category label=category value=category>
    <DropdownOption value="%" valueLabel="All Categories"/>
</Dropdown>

<Dropdown data={distinct_business} name=business label=business_name value=business_id>
    <DropdownOption value="%" valueLabel="All Businesses"/>
</Dropdown>

<Dropdown data={distinct_business} name=city label=city value=city>
    <DropdownOption value="%" valueLabel="All Cities"/>
</Dropdown>

<Dropdown data={distinct_business} name=state label=state value=state>
    <DropdownOption value="%" valueLabel="All States"/>
</Dropdown>

<BarChart
  data={orders_by_month}
  x=review_month
  y=total_reviews
  title = "Total Yelp Reviews by Month (2021)"
/>

```sql distinct_categories
select distinct category
from yelp.business_categories
```

```sql distinct_business
select distinct business_id, 
business_name, 
city, 
state
from yelp.business
```

```sql orders_by_month
with metrics as (
select *
from yelp.monthly_metrics as metrics

),
cats as (
    select category,
    business_id 
    from yelp.business_categories
),
metrics_by_cat as (
    select 
    metrics.business_id,
    metrics.business_name,
    metrics.review_month,
    metrics.total_reviews,
    metrics.avg_stars,
    cats.category
    from metrics
    left join cats on metrics.business_id = cats.business_id
    limit '10000'
)
select * 
from metrics_by_cat
where category like '${inputs.category.value}'
and business_id like '${inputs.business.value}'
```


