from pyspark.sql import SparkSession
from configs.spark.jobs.constants import schema_name, table_cars as table_name
from configs.spark.jobs.create_tables import create_cars_table
from configs.spark.jobs.generate_data import generate_cars_table_data

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("ModelCreation") \
    .getOrCreate() \

#2 Create the table
create_cars_table(spark, schema_name, table_name)

#3 Generate data
df = generate_cars_table_data(spark)

#4 Write DataFrame to Iceberg table
df.coalesce(1).writeTo(f"{schema_name}.{table_name}").append()

#5 Display the table contents
df = spark.sql(f"SELECT * FROM {schema_name}.{table_name}")
df.show()

#6 Stop the session
spark.stop()


# #docker exec -it spark bash
# #spark-submit 3_cars.py