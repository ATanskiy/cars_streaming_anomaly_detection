"""
Spark schema definitions for Kafka message parsing.
Defines structured schemas for raw sensor events (event metadata, car ID,
speed, RPM, gear) and enriched events (adds driver info, brand/model/color
names, and calculated expected gear). Used for parsing JSON messages from
Kafka streams with proper type casting and validation.
"""

from pyspark.sql.types import StructType, StructField, StringType, \
      IntegerType, LongType, TimestampType

SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("car_id", LongType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

ENRICHED_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("car_id", LongType(), True),
    StructField("driver_id", LongType(), True),
    StructField("brand_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("color_name", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("expected_gear", IntegerType(), True)
])