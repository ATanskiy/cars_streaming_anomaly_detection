"""
Iceberg table schema definitions and creation functions.
Defines and creates all project tables including dimension tables (car_models,
car_colors, cars), enriched sensor data table with daily partitioning and alert
flags, and raw ingestion table for audit trails. All tables use Iceberg format v2
with Snappy compression and S3 storage locations.
"""

def create_location(SCHEMA_NAME, table_name):
    return f's3a://spark/data/{SCHEMA_NAME}/{table_name}'

def create_car_models_table(spark, SCHEMA_NAME, table_name):

    table_location = create_location(SCHEMA_NAME, table_name)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
        model_id INT,
        car_brand STRING,
        car_model STRING
        )
        USING iceberg
        LOCATION '{table_location}'
        TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy'
        );
        """)
    
    # Truncate the table if it already exists
    spark.sql(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name}")
    print(f"Table {SCHEMA_NAME}.{table_name} created/truncated at {table_location}")


def create_car_colors_table(spark, SCHEMA_NAME, table_name):

    table_location = create_location(SCHEMA_NAME, table_name)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
        color_id INT,
        color_name STRING
        )
        USING iceberg
        LOCATION '{table_location}'
        TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy'
        );
        """)
    
    # Truncate the table if it already exists
    spark.sql(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name}")
    print(f"Table {SCHEMA_NAME}.{table_name} created/truncated at {table_location}")


def create_cars_table(spark, SCHEMA_NAME, table_name):

    table_location = create_location(SCHEMA_NAME, table_name)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{table_name} (
        car_id BIGINT,
        driver_id BIGINT,
        model_id INT,
        color_id INT
        )
        USING iceberg
        LOCATION '{table_location}'
        TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'snappy'
        )
        """)
    
    # Truncate the table if it already exists
    spark.sql(f"TRUNCATE TABLE {SCHEMA_NAME}.{table_name}")
    print(f"Table {SCHEMA_NAME}.{table_name} created/truncated at {table_location}")


def create_cars_enriched_table(spark, SCHEMA_NAME, table_name):

    table_location = create_location(SCHEMA_NAME, table_name)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dims.cars_enriched (
        event_id STRING,
        event_time TIMESTAMP,
        car_id BIGINT,
        driver_id BIGINT,
        brand_name STRING,
        model_name STRING,
        color_name STRING,
        speed INT,
        rpm INT,
        gear INT,
        expected_gear INT,
        is_alert BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        LOCATION '{table_location}'
        """)
    print(f"Table {SCHEMA_NAME}.{table_name} created at {table_location}")


def create_cars_raw_table(spark, SCHEMA_NAME, table_name):

    table_location = create_location(SCHEMA_NAME, table_name)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS cars_raw.cars_raw (
        id STRING,
        json_data STRING,
        timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp))
        LOCATION '{table_location}'
        """)
    print(f"Table {SCHEMA_NAME}.{table_name} created at {table_location}")