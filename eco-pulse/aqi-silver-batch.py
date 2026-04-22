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

silver_aqi_tb=f"{dbh.table_location()}.silver_aqi"
silver_aqi_2025_tb=f"{dbh.table_location()}.silver_2025_aqi"

# COMMAND ----------

from pyspark.sql.functions import year

silver_2025 = spark.read.table(silver_aqi_tb) \
    .withColumn("derived_year", year("date"))\
    .filter("derived_year = 2025")

# COMMAND ----------

silver_2025.write.mode("overwrite") \
    .saveAsTable(silver_aqi_2025_tb)
