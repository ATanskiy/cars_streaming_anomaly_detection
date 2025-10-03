from pyspark.sql import SparkSession
from configs.spark.jobs.constants import tables_to_compact, catalog_name, schema_name
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
for table_name in tables_to_compact:
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    print(f"\nCompacting {full_table_name}...")
    
    try:
        rewrite_data_files(spark, catalog_name, schema_name, table_name)
        spark.sparkContext._jvm.System.gc()

        rewrite_manifest_files(spark, catalog_name, schema_name, table_name)
        spark.sparkContext._jvm.System.gc()

        expire_old_snapshots(spark, catalog_name, schema_name, table_name)
        spark.sparkContext._jvm.System.gc()

        remove_orphan_files(spark, catalog_name, schema_name, table_name)
        print(f"✓ Compaction completed for {full_table_name}\n")
        success_count += 1
    except Exception as e:
        print(f"✗ Error compacting {full_table_name}: {str(e)}\n")
        continue

print("=" * 60)
print(f"Compaction Job Completed: {success_count}/{len(tables_to_compact)} tables succeeded")
print("=" * 60)

spark.stop()