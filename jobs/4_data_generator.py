"""
Kafka data generator for car sensor events.
Loads car IDs from the dims.cars table and continuously streams
random sensor data (speed, RPM, gear) to Kafka topic every second.
Used for testing real-time data processing pipelines.
"""

from pyspark.sql import SparkSession
import json, time, random, uuid
from kafka import KafkaProducer
from datetime import datetime
from configs.constants import TOPIC_SENSORS_SAMPLE, KAFKA_BOOTSTRAP_SERVER

#1 Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("DataGenerator") \
    .getOrCreate()

#2 Define the producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#3 Load the cars table
cars_df = spark.sql("SELECT car_id FROM dims.cars")
car_ids = [row.car_id for row in cars_df.collect()]

print(f"Loaded {len(car_ids)} cars from dims.cars table")

#4 Send random car events to Kafka
print(f"Starting to send data to Kafka topic: {TOPIC_SENSORS_SAMPLE}")
print("Sending one random car event every second...")

try:
    while True:
        car_id = random.choice(car_ids)
        event_data = {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now().isoformat(),
            "car_id": car_id,
            "speed": random.randint(0, 120),
            "rpm": random.randint(0, 8000),
            "gear": random.randint(1, 7)
        }

        producer.send(TOPIC_SENSORS_SAMPLE, value=event_data)
        print(f"Sent event: {event_data}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping data generation...")
finally:
    producer.close()
    spark.stop()
    print("Producer closed and Spark session stopped.")

# #docker exec -it spark bash
# #spark-submit 4_data_generator.py