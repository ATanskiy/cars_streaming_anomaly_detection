from pyspark.sql import SparkSession
from configs.constants import TABLES_TO_COMPACT, CATALOG_NAME, SCHEMA_NAME
from configs.spark.jobs.compaction_funcs import (
    rewrite_data_files,
    rewrite_manifest_files,
    expire_old_snapshots,
    remove_orphan_files
)

spark = SparkSession.builder \
    .appName("IcebergCompaction") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.memory.fraction", "0.6") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 60)
print("Starting Iceberg Table Compaction Job")
print("=" * 60)

success_count = 0
for table_name in TABLES_TO_COMPACT:
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    print(f"\nCompacting {full_table_name}...")
    
    try:
        rewrite_data_files(spark, CATALOG_NAME, SCHEMA_NAME, table_name)
        spark.sparkContext._jvm.System.gc()

        rewrite_manifest_files(spark, CATALOG_NAME, SCHEMA_NAME, table_name)
        spark.sparkContext._jvm.System.gc()

        expire_old_snapshots(spark, CATALOG_NAME, SCHEMA_NAME, table_name)
        spark.sparkContext._jvm.System.gc()

        remove_orphan_files(spark, CATALOG_NAME, SCHEMA_NAME, table_name)
        print(f"✓ Compaction completed for {full_table_name}\n")
        success_count += 1
    except Exception as e:
        print(f"✗ Error compacting {full_table_name}: {str(e)}\n")
        continue

print("=" * 60)
print(f"Compaction Job Completed: {success_count}/{len(TABLES_TO_COMPACT)} tables succeeded")
print("=" * 60)

spark.stop()