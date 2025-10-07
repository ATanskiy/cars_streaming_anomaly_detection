"""
Real-time alert aggregation and Telegram notification pipeline.
Reads alert events from Kafka, aggregates them in 1-minute windows with
10-second watermark, calculates alert counts by car color and maximum sensor
values (speed, gear, RPM), and sends aggregated summaries to Telegram using
foreachBatch processing for batch notifications.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, window, \
    count, sum as spark_sum, max as spark_max
from pyspark.sql.types import TimestampType
from configs.constants import TOPIC_ALERT_DATA as TOPIC_INPUT, KAFKA_BOOTSTRAP_SERVERS
from configs.spark.jobs.schemas import ENRICHED_SCHEMA
from jobs.tg_functions_job_7 import send_batch

#1 Create Spark session
spark = SparkSession.builder \
    .appName("AlertCounter") \
    .getOrCreate()

#2 Listen to enriched data from Kafka
samples_enriched = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_INPUT) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

#3 Parse the enriched data
parsed_alerts_df = samples_enriched \
    .select(from_json(col("value").cast("string"), ENRICHED_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("event_time").cast(TimestampType()))

#4 Aggregate
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

#5 Write stream - USE foreachBatch instead of console!
query = aggregated_df \
    .writeStream \
    .foreachBatch(send_batch) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", "/tmp/checkpoints/alerts_telegram") \
    .start()

#6 Await termination
query.awaitTermination()