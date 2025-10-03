from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, window, \
    count, sum as spark_sum, max as spark_max
from pyspark.sql.types import TimestampType
from configs.constants import TOPIC_ALERT_DATA as TOPIC_INPUT, KAFKA_BOOTSTRAP_SERVERS
from configs.spark.jobs.schemas import ENRICHED_SCHEMA

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("AlertCounter") \
    .getOrCreate()

#2 Listen to enriched data from Kafka
samples_enriched = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_INPUT) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

#3 Parse the enriched data
parsed_alerts_df = samples_enriched \
    .select(from_json(col("value").cast("string"), ENRICHED_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("event_time").cast(TimestampType()))

#4 Create an Iceberg table for alerts if not exists
aggregated_df = parsed_alerts_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute")) \
    .agg(
        count("*").alias("num_of_rows"),
        spark_sum(when(col("color_name") == "Black", 1).otherwise(0)).alias("num_of_black"),
        spark_sum(when(col("color_name") == "White", 1).otherwise(0)).alias("num_of_white"),
        spark_sum(when(col("color_name") == "Gray", 1).otherwise(0)).alias("num_of_silver"),
        spark_max("speed").alias("maximum_speed"),
        spark_max("gear").alias("maximum_gear"),
        spark_max("rpm").alias("maximum_rpm")
    )

#5 Display data to console
query = aggregated_df \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoints/alerts") \
    .start()

#6 Await termination of both streams
query.awaitTermination()

# #docker exec -it spark bash
# #spark-submit 7_print_aggregations.py