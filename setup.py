from setuptools import find_packages, setup

setup(
    name="dagster_capstone",
    packages=find_packages(exclude=["dagster_capstone_tests"]),
    install_requires=[
        "dagster==1.6.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-aws",
        "dagster-pyspark",
        "geopandas",
        "kaleido",
        "pandas",
        "polars",
        "s3fs",
        "kaggle"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
