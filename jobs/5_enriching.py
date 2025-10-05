from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr,\
      broadcast, round as spark_round, to_json, struct, current_timestamp
from pyspark.sql.types import IntegerType
from configs.spark.jobs.schemas import SENSOR_SCHEMA
from configs.constants import TOPIC_SENSORS_SAMPLE as TOPIC_INPUT, \
    TOPIC_SENSORS_ENRICHED as TOPIC_OUTPUT, KAFKA_BOOTSTRAP_SERVERS

#1 Create Spark session
spark = SparkSession.builder \
    .appName("DataEnrichment") \
    .getOrCreate()

#2 Load the tables for enrichment
cars_df = spark.sql("SELECT car_id, driver_id, model_id, color_id FROM dims.cars")
car_models_df = spark.sql("SELECT model_id, car_brand, car_model FROM dims.car_models")
car_colors_df = spark.sql("SELECT color_id, color_name FROM dims.car_colors")

#3 Read from Kafka and parse JSON
sensor_samples = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_INPUT) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

raw_sensor_samples = sensor_samples.select(
    expr("uuid()").alias("id"),
    col("value").cast("string").alias("json_data"), 
    current_timestamp().alias("timestamp")
)

cars_parsed_df = sensor_samples \
    .select(from_json(col("value").cast("string"), schema=SENSOR_SCHEMA).alias("data")) \
    .select("data.*")

#4 Enrich the data by joining with the dimension tables
cars_enriched_df = cars_parsed_df \
    .join(broadcast(cars_df), on="car_id", how="left") \
    .join(broadcast(car_models_df), on="model_id", how="left") \
    .join(broadcast(car_colors_df), on="color_id", how="left") \
    .withColumn("expected_gear", spark_round(col("speed") / 30).cast(IntegerType())) \
    .select(
        "event_id",
        "event_time",
        "car_id",
        "driver_id",
        col("car_brand").alias("brand_name"),
        col("car_model").alias("model_name"),
        col("color_name"),
        "speed",
        "rpm",
        "gear",
        "expected_gear"
    )

#5 Push enriched data to Kafka
query1 = cars_enriched_df \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", TOPIC_OUTPUT) \
    .option("checkpointLocation", f"s3a://spark/data/checkpoints/{TOPIC_OUTPUT}") \
    .outputMode("append") \
    .start()

#6 write raw data into an iceberg table
query2 = raw_sensor_samples \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://spark/data/checkpoints/cars-raw") \
    .toTable("cars_raw.cars_raw")

#7 keep the queries up
query1.awaitTermination()
query2.awaitTermination()

# #docker exec -it spark bash
# #spark-submit 5_enriching.py
