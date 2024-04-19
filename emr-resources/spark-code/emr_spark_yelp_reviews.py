from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, year, month

spark = SparkSession\
        .builder\
        .appName("YelpReviews")\
        .getOrCreate()

df = spark.read.json("s3://your-bucket/yelp/raw/yelp_academic_dataset_review.json") 
#json_df.printSchema()

df = df.withColumnRenamed('date', 'datetime_temp') \
       .withColumn('datetime', to_timestamp('datetime_temp')) \
       .drop('datetime_temp')

# Extract date, year, and month from 'datetime' and create new columns
df = df.withColumn('date', col('datetime').cast('date')) \
       .withColumn('year', year(col('datetime'))) \
       .withColumn('month', month(col('datetime')))

s3_path = 's3://de-capstone-project/yelp/processed/reviews/'

df.write.partitionBy('year', 'month').parquet(s3_path, mode='overwrite')