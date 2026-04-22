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
gold_schema_dir=f"{dbh.schemas_directory()}/gold_tb"
os.makedirs(gold_schema_dir, exist_ok=True)
gold_checkpoints_dir=f"{dbh.checkpoints_directory()}/gold_tb"
os.makedirs(gold_checkpoints_dir, exist_ok=True)

silver_aqi_tb=f"{dbh.table_location()}.silver_aqi"
gold_aqi_tb=f"{dbh.table_location()}.gold_aqi"
gold_aqi_batch_tb=f"{dbh.table_location()}.gold_aqi_batch"
silver_aqi_batch=f"{dbh.table_location()}.silver_2025_aqi"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Leg - for source inference

# COMMAND ----------

from pyspark.sql.functions import avg

daily_batch = spark.table(silver_aqi_batch).groupBy("city", "date") \
    .pivot("parameter") \
    .agg(avg("value"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Inferences
# MAGIC
# MAGIC #### 🚗 1. Traffic Emission Inference
# MAGIC - **Logic:** Traffic → NO₂ + NOx + CO high, especially during rush hours  
# MAGIC - **Rule:** IF NO₂ ↑ AND CO ↑ AND NOx ↑ → Likely traffic emissions  
# MAGIC - **Spark rule:**  
# MAGIC   `traffic_flag = when((col("no2") > 40) & (col("co") > 500) & (col("nox") > 50), 1).otherwise(0)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🏭 2. Industrial / Power Plant Signal
# MAGIC - **Logic:** Industries → SO₂ + NOx high  
# MAGIC - **Rule:** IF SO₂ ↑ AND NOx ↑ → Industrial / coal combustion  
# MAGIC - **Spark rule:**  
# MAGIC   `industrial_flag = when((col("so2") > 20) & (col("nox") > 40), 1).otherwise(0)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🌾🔥 3. Biomass / Crop Burning
# MAGIC - **Logic:** Burning → PM2.5 ↑ + CO ↑ + low wind  
# MAGIC - **Rule:** IF PM2.5 ↑ AND CO ↑ AND wind_speed low → Biomass burning  
# MAGIC - **Spark rule:**  
# MAGIC   `biomass_flag = when((col("pm25") > 100) & (col("co") > 400) & (col("wind_speed") < 2), 1).otherwise(0)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🌪️ 4. Dust / Construction (Coarse Particles)
# MAGIC - **Logic:** Dust → PM10 >> PM2.5, often windy  
# MAGIC - **Rule:** IF PM10 high AND PM2.5/PM10 < 0.5 → Dust / construction  
# MAGIC - **Spark rule:**  
# MAGIC   `dust_flag = when((col("pm10") > 150) & (col("pm25")/col("pm10") < 0.5), 1).otherwise(0)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### ☀️ 5. Photochemical Smog (Ozone)
# MAGIC - **Logic:** O₃ forms with sunlight (temperature ↑), NOx present  
# MAGIC - **Rule:** IF O₃ ↑ AND temperature ↑ → photochemical smog  
# MAGIC - **Spark rule:**  
# MAGIC   `ozone_flag = when((col("o3") > 100) & (col("temperature") > 30), 1).otherwise(0)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🌧️ 6. Weather Cleansing Effect
# MAGIC - **Logic:** High humidity / rain → pollutants drop  
# MAGIC - **Insight:** High humidity → AQI drop  
# MAGIC - **Spark rule:**  
# MAGIC   `cleansing_flag = when((col("relativehumidity") > 80) & (col("pm25") < 50), 1).otherwise(0)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🌬️ 7. Wind Transport (Pollution Movement)
# MAGIC - **Logic:** Wind direction + speed → pollution transport  
# MAGIC - **Insight:** (Rule to be defined)

# COMMAND ----------

from pyspark.sql.functions import col, when
daily_batch = daily_batch.withColumn(
    "source",
    when((col("no2") > 40) & (col("co") > 500) & (col("nox") > 50), "Traffic")
    .when((col("so2") > 20) & (col("nox") > 40), "Industrial")
    .when((col("pm25") > 100) & (col("co") > 400) & (col("wind_speed") < 2), "Biomass")
    .when((col("pm10") > 150) & (col("pm25")/col("pm10") < 0.5), "Dust")
    .when((col("o3") > 100) & (col("temperature") > 30), "Ozone")
    .when((col("relativehumidity") > 80) & (col("pm25") < 50), "WeatherCleansing")
    .otherwise("Mixed")
)

# COMMAND ----------

daily_batch.write.mode("overwrite") \
    .saveAsTable(gold_aqi_batch_tb)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Speed Leg
# MAGIC - no pivot possible or group by
# MAGIC - should be fast

# COMMAND ----------

silver = spark.readStream.table(silver_aqi_tb)

pm25_df = silver.filter(col("parameter") == "pm25")

# COMMAND ----------

daily = pm25_df.groupBy("city", "date") \
    .agg(avg("value").alias("pm25"))

# COMMAND ----------

daily = daily.withColumn(
    "severity",
    when(col("pm25") > 250, "Severe")
    .when(col("pm25") > 150, "Unhealthy")
    .when(col("pm25") > 100, "Moderate")
    .otherwise("Good")
)

# COMMAND ----------

daily = daily.withColumn(
    "clean_flag",
    (col("pm25") < 50).cast("int")
)

# COMMAND ----------

daily = daily.withColumn(
    "unhealthy_flag",
    (col("pm25") > 150).cast("int")
)

# COMMAND ----------

daily = daily.withColumn(
    "aqi_bucket",
    when(col("pm25") < 50, "Good")
    .when(col("pm25") < 100, "Satisfactory")
    .when(col("pm25") < 150, "Moderate")
    .when(col("pm25") < 200, "Poor")
    .when(col("pm25") < 300, "Very Poor")
    .otherwise("Severe")
)

# COMMAND ----------

daily = daily.withColumn("date", col("date").cast("timestamp")).withWatermark("date", "2 days")

# COMMAND ----------

(daily.writeStream
    .format("delta")
    .option("checkpointLocation", gold_checkpoints_dir)
    .partitionBy("date")
    .outputMode("complete")   # because aggregation
    .toTable(gold_aqi_tb)
)
