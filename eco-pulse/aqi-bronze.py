# Databricks notebook source
# MAGIC %pip install ./databricks_helpers

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks_helpers.databricks_helper import DatabricksHelpers
dbh = DatabricksHelpers(dbutils, "aura_india",spark)

# COMMAND ----------

import os

bronze_streaming_simulation=f"{dbh.volume_directory()}/raw_openaq_stream"
os.makedirs(bronze_streaming_simulation, exist_ok=True)
aqi_bronze_schema_dir=f"{dbh.schemas_directory()}/openaq_bronze"
os.makedirs(aqi_bronze_schema_dir, exist_ok=True)
aqi_bronze_checkpoints_dir=f"{dbh.checkpoints_directory()}/openaq_bronze"
os.makedirs(aqi_bronze_checkpoints_dir, exist_ok=True)

bronze_aqi_tb=f"{dbh.table_location()}.bronze_aqi"

# COMMAND ----------

from pyspark.sql.functions import col

bronze_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("header", True) \
    .option("cloudFiles.schemaLocation", aqi_bronze_schema_dir) \
    .option("cloudFiles.rescuedDataColumn", "_rescued_data_bronze") \
    .load(bronze_streaming_simulation)\
    .select(
        "*", # Keep all original columns
        col("_metadata.file_modification_time").alias("file_arrival_timestamp"),
        col("_metadata.file_path").alias("source_file_path")
    )

# COMMAND ----------

(bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", aqi_bronze_checkpoints_dir)
    .outputMode("append")
    .option("mergeSchema", "true") #tells to accept new schema
    .table(bronze_aqi_tb)
)
