# Catalogs/Schemas names
catalog_name = "iceberg"
schema_name = "dims"

# Table names
table_car_models = "car_models"
table_car_colors = "car_colors"
table_cars = "cars"
table_cars_enriched = "cars_enriched"

# Cars table columns
table_car_models_columns = ["model_id", "car_brand", "car_model"]
table_car_colors_columns =  ["color_id", "color_name"]
table_cars_columns = ["car_id", "driver_id", "model_id", "color_id"]

# Topics
topic_sensors_sample = 'sensors-sample'
topic_sensors_enriched = 'samples-enriched'
topic_alert_data = 'alert-data'


# Kafka
kafka_bootstrap_server = 'kafka:9092'
kafka_bootstrap_servers = "kafka_broker:9092"

# Compaction parameters
tables_to_compact = [
        "cars_enriched"
    ]