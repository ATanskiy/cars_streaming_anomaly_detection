from datetime import datetime, timedelta

def rewrite_data_files(spark, CATALOG_NAME, SCHEMA_NAME, table_name):
    """Rewrite data files - consolidates small files into larger ones."""
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    
    print(f"Starting data file compaction for {full_table_name}...")
    spark.sql(f"""
        CALL {CATALOG_NAME}.system.rewrite_data_files(
            table => '{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}',
            options => map(
                'target-file-size-bytes', '134217728',
                'min-file-size-bytes', '67108864',
                'partial-progress.enabled', 'true',
                'partial-progress.max-commits', '10'
            )
        )
    """)
    print(f"✓ Data files compacted for {full_table_name}")


def rewrite_manifest_files(spark, CATALOG_NAME, SCHEMA_NAME, table_name):
    """Rewrite manifest files."""
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    
    spark.sql(f"""
        CALL {CATALOG_NAME}.system.rewrite_manifests('{SCHEMA_NAME}.{table_name}')
    """)
    print(f"✓ Manifest files rewritten for {full_table_name}")


def expire_old_snapshots(spark, CATALOG_NAME, SCHEMA_NAME, table_name, days=1, retain_last=3):
    """Expire old snapshots."""
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    expire_timestamp = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    
    spark.sql(f"""
        CALL {CATALOG_NAME}.system.expire_snapshots(
            table => '{SCHEMA_NAME}.{table_name}',
            older_than => TIMESTAMP '{expire_timestamp}',
            retain_last => {retain_last}
        )
    """)
    print(f"✓ Snapshots expired for {full_table_name} (older than {days} days, kept last {retain_last})")


def remove_orphan_files(spark, CATALOG_NAME, SCHEMA_NAME, table_name):
    """Remove orphan files."""
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    
    spark.sql(f"""
        CALL {CATALOG_NAME}.system.remove_orphan_files(
            table => '{SCHEMA_NAME}.{table_name}',
            older_than => TIMESTAMP '2999-01-01 00:00:00'
        )
    """)
    print(f"✓ Orphan files removed for {full_table_name}")