from pyspark.sql import SparkSession
from configs.spark.jobs.constants import schema_name, table_car_colors as table_name
from configs.spark.jobs.create_tables import create_car_colors_table
from configs.spark.jobs.generate_data import generate_car_colors_data

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("ModelCreation") \
    .getOrCreate() \

#2 Create the table
create_car_colors_table(spark, schema_name, table_name)

#3 Populate the table
df = generate_car_colors_data(spark)
df.coalesce(1).writeTo(f"{schema_name}.{table_name}").append()

#3 Display the table contents
df = spark.sql(f"SELECT * FROM {schema_name}.{table_name}")
df.show()

#4 Stop the session
spark.stop()


# #docker exec -it spark bash
# #spark-submit 2_car_colors.py