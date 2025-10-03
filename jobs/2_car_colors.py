from pyspark.sql import SparkSession
from configs.constants import SCHEMA_NAME, TABLE_CAR_COLORS as TABLE_NAME
from configs.spark.jobs.create_tables import create_car_colors_table
from configs.spark.jobs.generate_data import generate_car_colors_data

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("ModelCreation") \
    .getOrCreate() \

#2 Create the table
create_car_colors_table(spark, SCHEMA_NAME, TABLE_NAME)

#3 Populate the table
df = generate_car_colors_data(spark)
df.coalesce(1).writeTo(f"{SCHEMA_NAME}.{TABLE_NAME}").append()

#3 Display the table contents
df = spark.sql(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}")
df.show()

#4 Stop the session
spark.stop()


# #docker exec -it spark bash
# #spark-submit 2_car_colors.py