# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Databricks Auto Loader Pipeline
# MAGIC 
# MAGIC This notebook demonstrates a simple Auto Loader pipeline that:
# MAGIC - Loads data from S3 (any supported format)
# MAGIC - Dynamically determines the target table from the file name (e.g., edm_entity_2024-06-01.csv → edm_entity)
# MAGIC - Performs checkpointing
# MAGIC - Adds source file and load timestamp columns
# MAGIC - Moves processed files to an S3 archive zone
# MAGIC - Only loads into tables that exist in the bronze schema of the entity_resolution_dev catalog

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp, lit
import re

# CONFIGURATION
landing_zone = 's3://your-bucket/landing-zone/'
archive_zone = 's3://your-bucket/archive-zone/'
catalog = 'entity_resolution_dev'
schema = 'bronze'
checkpoint_path = '/tmp/autoloader/checkpoints'

# Helper: List all tables in the schema
tables = [t.name for t in spark.catalog.listTables(f'{catalog}.{schema}')]
print('Available tables:', tables)

# Helper: Extract table name from file name
def extract_table_name(file_path):
    # Example: s3://.../edm_entity_2024-06-01.csv -> edm_entity
    match = re.search(r'/([a-zA-Z0-9_]+)[^/]*\.', file_path)
    if match:
        return match.group(1)
    else:
        return None

# Helper: Detect file format from extension
def detect_format(file_path):
    ext = file_path.split('.')[-1].lower()
    if ext in ['csv', 'json', 'parquet', 'txt']:
        return ext
    return 'csv'  # default

print('Configuration loaded. Ready to process files.')

# COMMAND ----------

# Main: Process all new files in landing zone
files = dbutils.fs.ls(landing_zone)
for file in files:
    file_path = file.path
    table_name = extract_table_name(file_path)
    if not table_name:
        print(f'Could not extract table name from {file_path}, skipping.')
        continue
    if table_name not in tables:
        print(f'Table {table_name} does not exist in {catalog}.{schema}, skipping.')
        continue

    file_format = detect_format(file_path)
    print(f'Processing {file_path} as {file_format} into {catalog}.{schema}.{table_name}')

    # Set options for Auto Loader
    options = {
        'cloudFiles.format': file_format,
        'cloudFiles.schemaLocation': f'{checkpoint_path}/{table_name}/schema',
        'cloudFiles.inferColumnTypes': 'true'
    }
    if file_format == 'csv':
        options['header'] = 'true'
        options['delimiter'] = ','
    if file_format == 'txt':
        options['header'] = 'true'
        options['delimiter'] = '\t'

    # Read with Auto Loader
    df = (
        spark.read.format('cloudFiles')
        .options(**options)
        .load(file_path)
        .withColumn('source_file', lit(file_path))
        .withColumn('load_timestamp', current_timestamp())
    )

    # Write to Delta table
    df.write.format('delta') \
        .mode('append') \
        .option('mergeSchema', 'true') \
        .saveAsTable(f'{catalog}.{schema}.{table_name}')

    # Move file to archive
    archive_path = archive_zone + file_path.replace(landing_zone, '')
    dbutils.fs.mv(file_path, archive_path)
    print(f'Moved {file_path} to {archive_path}')

print('Auto Loader pipeline complete.') 