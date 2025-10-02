def create_location(schema_name, table_name):
    return f's3a://spark/data/{schema_name}/{table_name}'

def create_car_models_table(spark, schema_name, table_name):

    table_location = create_location(schema_name, table_name)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
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
    spark.sql(f"TRUNCATE TABLE {schema_name}.{table_name}")
    print(f"Table {schema_name}.{table_name} created/truncated at {table_location}")


def create_car_colors_table(spark, schema_name, table_name):

    table_location = create_location(schema_name, table_name)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
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
    spark.sql(f"TRUNCATE TABLE {schema_name}.{table_name}")
    print(f"Table {schema_name}.{table_name} created/truncated at {table_location}")


def create_cars_table(spark, schema_name, table_name):

    table_location = create_location(schema_name, table_name)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
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
    spark.sql(f"TRUNCATE TABLE {schema_name}.{table_name}")
    print(f"Table {schema_name}.{table_name} created/truncated at {table_location}")


def create_cars_enriched_table(spark, schema_name, table_name):

    table_location = create_location(schema_name, table_name)

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
    print(f"Table {schema_name}.{table_name} created at {table_location}")