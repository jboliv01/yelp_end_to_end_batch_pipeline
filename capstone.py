import os
import duckdb
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(layout="wide")
# Connect to DuckDB using Motherduck
con = duckdb.connect(database=f'md:yelp?motherduck_token={os.getenv("MOTHERDUCK_TOKEN")}', read_only=True)

# SQL Query to fetch data
query = """
with metrics as (
    select *
    from yelp.fact_monthly_metrics as metrics
),
cats as (
    select category, business_id
    from yelp.dim_yelp_business_categories
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
)
select * from metrics_by_cat
"""
df = con.execute(query).df()

# App Title
st.title("Yelp Business Reviews Dashboard")

# Displaying KPIs
st.subheader("Key Performance Indicators")

# KPI 1: Total Reviews
total_reviews = df['total_reviews'].sum()
st.metric(label="Total Reviews", value=f"{total_reviews:,}")

# KPI 2: Average Stars
average_stars = df['avg_stars'].mean()
st.metric(label="Average Stars", value=f"{average_stars:.2f}")

# Charts in a single row with two columns
col1, col2 = st.columns([5, 3])

with col1:
    # Monthly Reviews Line Graph
    st.subheader("Monthly Reviews Over Time")
    df_monthly = df.groupby('review_month')['total_reviews'].sum().reset_index()
    st.line_chart(df_monthly.set_index('review_month'))

with col2:
    # Categories Pie Chart
    st.subheader("Top 10 Business Categories Distribution")
    # Calculate top 10 categories
    df_categories = df.groupby('category')['business_id'].nunique().nlargest(10).reset_index()
    df_categories.columns = ['Category', 'Count']

    # Create a pie chart
    fig, ax = plt.subplots()
    ax.pie(df_categories['Count'], labels=df_categories['Category'], autopct='%1.1f%%', startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    # Display the pie chart
    st.pyplot(fig)

# # Optional: Displaying the table for a closer look
# st.subheader("Detailed Data View")
# st.dataframe(df)
