# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %pip install ./databricks_helpers

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks_helpers.databricks_helper import DatabricksHelpers
dbh = DatabricksHelpers(dbutils, "aura_india",spark)

# COMMAND ----------

import os
silver_schema_dir=f"{dbh.schemas_directory()}/silver_tb"
os.makedirs(silver_schema_dir, exist_ok=True)
silver_checkpoints_dir=f"{dbh.checkpoints_directory()}/silver_tb"
os.makedirs(silver_checkpoints_dir, exist_ok=True)

indian_cities_tb=f"{dbh.table_location()}.indian_cities"
city_location_mapping_25_tb=f"{dbh.table_location()}.city_location_25_mapping"
bronze_aqi_tb=f"{dbh.table_location()}.bronze_aqi"
silver_aqi_tb=f"{dbh.table_location()}.silver_aqi"

# COMMAND ----------

spark.table(city_location_mapping_25_tb).filter("status='SUCCESS'").display()

# COMMAND ----------

spark.table(bronze_aqi_tb).display()

# COMMAND ----------

mapping_df = spark.table(
    city_location_mapping_25_tb
).filter("status = 'SUCCESS'")

# COMMAND ----------

# Setting data type and also prep for aggregating it for a day.
from pyspark.sql.functions import to_timestamp, to_date, col

df = bronze \
    .withColumn("timestamp", to_timestamp("datetime")) \
    .withColumn("date", to_date("timestamp"))\
    .withColumn("value", col("value").cast("double"))

# COMMAND ----------

# Joining tables

silver_df = df.join(
    mapping_df,
    df.location_id == mapping_df.location_id,
    "left"
)

# COMMAND ----------

silver_df = silver_df.select(
    "city",
    "latitude",
    "longitude",
    "distance",
    "locationid",
    "timestamp",
    "date",
    "year",
    "parameter",
    "value"
)

# COMMAND ----------

(silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoints_dir)
    .partitionBy("city")
    .toTable(silver_aqi_tb)
)

# COMMAND ----------

spark.table(bronze_aqi_tb).select("parameter").distinct().display()
