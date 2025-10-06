"""
Iceberg Table Maintenance and Compaction Job

Comprehensive maintenance pipeline for Apache Iceberg tables that optimizes storage,
query performance, and manages metadata lifecycle. Accepts schema and table names as 
command-line arguments for flexible execution across different tables.

Operations performed in sequence:
1. Data File Compaction - Consolidates small files from streaming writes into 128MB 
   target files to improve query performance and reduce metadata overhead
2. Manifest File Rewriting - Optimizes manifest metadata by consolidating manifest 
   files, reducing metadata read overhead during query planning
3. Snapshot Expiration - Removes old table snapshots (older than 1 day, retaining 
   last 3) to prevent unbounded metadata growth while maintaining time-travel capability
4. Orphan File Removal - Deletes unreferenced data files from storage that are no 
   longer tracked by any snapshot (older than 1 day), reclaiming disk space

Memory management: Explicit garbage collection between operations prevents memory 
buildup during long-running maintenance jobs.
"""

import sys
from pyspark.sql import SparkSession
from configs.constants import CATALOG_NAME
from configs.spark.jobs.compaction_funcs import (
    rewrite_data_files,
    rewrite_manifest_files,
    expire_old_snapshots,
    remove_orphan_files
)

# Get schema and table from command line arguments
if len(sys.argv) != 3:
    print("Usage: spark-submit script.py <schema_name> <table_name>")
    sys.exit(1)

SCHEMA_NAME = sys.argv[1]
TABLE_NAME = sys.argv[2]

spark = SparkSession.builder \
    .appName(f"IcebergCompaction_{SCHEMA_NAME}_{TABLE_NAME}") \
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
print(f"Starting Iceberg Table Compaction: {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
print("=" * 60)

full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

try:
    rewrite_data_files(spark, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    spark.sparkContext._jvm.System.gc()

    rewrite_manifest_files(spark, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    spark.sparkContext._jvm.System.gc()

    expire_old_snapshots(spark, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    spark.sparkContext._jvm.System.gc()

    remove_orphan_files(spark, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    spark.sparkContext._jvm.System.gc()

    print(f"✓ Compaction completed for {full_table_name}\n")
except Exception as e:
    print(f"✗ Error compacting {full_table_name}: {str(e)}\n")
    spark.stop()
    sys.exit(1)

print("=" * 60)
print(f"Compaction Job Completed Successfully")
print("=" * 60)

spark.stop()