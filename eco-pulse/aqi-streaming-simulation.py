# Databricks notebook source
# 1. Define the parameters (Default value, Widget Name, UI Label)
dbutils.widgets.text("year", "", "Year to process") # There are various widgets to choose from like Multiselect,etc
# 2. Get the string value from the workflow
year = dbutils.widgets.get("year")


# COMMAND ----------

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
raw_openaq_data=f"{dbh.volume_directory()}/raw_openaq_data"
os.makedirs(raw_openaq_data, exist_ok=True)
bronze_streaming_simulation=f"{dbh.volume_directory()}/raw_openaq_stream"
os.makedirs(bronze_streaming_simulation, exist_ok=True)


indian_cities_tb=f"{dbh.table_location()}.indian_cities"
city_location_mapping_25_tb=f"{dbh.table_location()}.city_location_25_mapping"
bronze_aqi_tb=f"{dbh.table_location()}.bronze_aqi"

# COMMAND ----------

#year to copy
year=2022
dbutils.fs.cp(f"{raw_openaq_data}/year={year}/", f"{bronze_streaming_simulation}/year={year}/", recurse=True)


# COMMAND ----------


