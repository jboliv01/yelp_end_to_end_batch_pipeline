from setuptools import find_packages, setup

setup(
    name="dagster_capstone",
    packages=find_packages(exclude=["dagster_capstone_tests"]),
    python_requires=">=3.8,<=3.11",
    install_requires=[
        "dagster==1.6.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-dbt",
        "dbt-duckdb",
        "duckdb==0.9.2",
        "dagster-aws",
        "dagster-pyspark",
        "dagster-polars",
        "geopandas",
        "kaleido",
        "pandas",
        "s3fs",
        "smart_open",
        "smart_open[s3]",
        "kaggle",
        "boto3",
        "pyarrow",
        "fastparquet",
        "streamlit"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
