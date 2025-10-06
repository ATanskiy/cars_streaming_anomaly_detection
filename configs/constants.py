# Catalogs/Schemas names
CATALOG_NAME = "iceberg"
SCHEMA_NAME = "dims"
RAW_SCHEMA_NAME = 'cars_raw'

# Table names
TABLE_CAR_MODELS = "car_models"
TABLE_CAR_COLORS = "car_colors"
TABLE_CARS = "cars"
TABLE_CARS_ENRICHED = "cars_enriched"
TABLE_CARS_RAW = "cars_raw"

# Cars table columns
TABLE_CAR_MODELS_COLUMNS = ["model_id", "car_brand", "car_model"]
TABLE_CAR_COLORS_COLUMNS =  ["color_id", "color_name"]
TABLE_CARS_COLUMNS = ["car_id", "driver_id", "model_id", "color_id"]

# Topics
TOPIC_SENSORS_SAMPLE = 'sensors-sample'
TOPIC_SENSORS_ENRICHED = 'samples-enriched'
TOPIC_ALERT_DATA = 'alert-data'


# Kafka
KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'
KAFKA_BOOTSTRAP_SERVERS = "kafka_broker:9092"

# Compaction parameters
TABLES_TO_COMPACT = {
    "dims": "cars_enriched",
    "cars_raw": "cars_raw"
}

# Superset constants
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
PERMANENT_SESSION_LIFETIME = 86400

FEATURE_FLAGS = {
    'DECK_GL': True,
    'DASHBOARD_NATIVE_FILTERS': True,
}

SQLLAB_HIDE_DATABASES = ["PostgreSQL"]
TALISMAN_ENABLED = False
CONTENT_SECURITY_POLICY_WARNING = False
ENABLE_CORS = True

CORS_OPTIONS = {
    "supports_credentials": True,
    "origins": ["*"],
}