"""
# My first app
Here's our first attempt at using data to create a table:
"""
import os
import duckdb
import streamlit as st
con = duckdb.connect(database=f'md:yelp?motherduck_token={os.getenv("MOTHERDUCK_TOKEN")}', read_only=True)

import pandas as pd
# df = pd.DataFrame({
#   'first column': [1, 2, 3, 4],
#   'second column': [10, 20, 30, 40]
# })

# Query for filtered data
query = """
with metrics as (
select *
from yelp.fact_monthly_metrics as metrics

),
cats as (
    select category,
    business_id 
    from yelp.dim_yelp_business_categories),
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
    )
select * 
from metrics_by_cat
"""
df = con.execute(query).df()

# Line Graph of Downloads Over Time
st.subheader("Monthly Reviews Over Time")
df_monthly = df.groupby('review_month')['total_reviews'].sum().reset_index()
st.line_chart(df_monthly.set_index('review_month'))
