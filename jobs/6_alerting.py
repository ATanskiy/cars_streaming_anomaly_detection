from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, when
from configs.spark.jobs.constants import topic_sensors_enriched as topic_input, \
    topic_alert_data as topic_output, kafka_bootstrap_servers, schema_name, \
        table_cars_enriched as table_name
from configs.spark.jobs.schemas import enriched_schema
from configs.spark.jobs.create_tables import create_cars_enriched_table

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("AlertDetection") \
    .getOrCreate()

#2 Listen to enriched data from Kafka
samples_enriched = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_input) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

#3 Parse the enriched data
parsed_enriched_df = samples_enriched \
    .select(from_json(col("value").cast("string"), enriched_schema).alias("data")) \
    .select("data.*") \
    .withColumn("is_alert", 
        when(
            (col("speed") > 120) | 
            (col("expected_gear") != col("gear")) | 
            (col("rpm") > 6000),
            True
        ).otherwise(False)
    )

#4 Create an Iceberg table for alerts if not exists
create_cars_enriched_table(spark, schema_name, table_name)

#5 Write data with alerts to the Iceberg table
query1 = parsed_enriched_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://spark/data/checkpoints/cars-enriched") \
    .toTable("dims.cars_enriched")

#6 Filter and show alerts in the console
alert_df = parsed_enriched_df.filter(col("is_alert") == True) \
    .drop("is_alert")

query2 = alert_df \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", topic_output) \
    .option("checkpointLocation", f"s3a://spark/data/checkpoints/{topic_output}") \
    .outputMode("append") \
    .start()

#7 Await termination of both streams
query1.awaitTermination()
query2.awaitTermination()

# #docker exec -it spark bash
# #spark-submit 6_alerting.py