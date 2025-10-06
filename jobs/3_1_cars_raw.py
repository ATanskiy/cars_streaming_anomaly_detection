"""
Creates the cars_raw Iceberg table schema.
Initializes an empty raw data table without populating it with data.
Used as a landing zone for raw car data ingestion.
"""

from pyspark.sql import SparkSession
from configs.constants import RAW_SCHEMA_NAME as SCHEMA_NAME, \
    TABLE_CARS_RAW as TABLE_NAME
from configs.spark.jobs.create_tables import create_cars_raw_table

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("ModelCreation") \
    .getOrCreate() \

#2 Create the table
create_cars_raw_table(spark, SCHEMA_NAME, TABLE_NAME)

#3 Display the table contents
df = spark.sql(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}")
df.show()

#4 Stop the session
spark.stop()


# #docker exec -it spark bash
# #spark-submit 3_1_cars.py