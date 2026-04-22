# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %pip install ./databricks_helpers openaq

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks_helpers.databricks_helper import DatabricksHelpers
dbh = DatabricksHelpers(dbutils, "aura_india",spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension table
# MAGIC #### [Indian cities Lat & Long](https://download.geonames.org/export/dump/) 
# MAGIC [Readme.txt](https://download.geonames.org/export/dump/readme.txt)
# MAGIC - geonameid         : integer id of record in geonames database
# MAGIC - name              : name of geographical point (utf8) varchar(200)
# MAGIC - asciiname         : name of geographical point in plain ascii characters, varchar(200)
# MAGIC - alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
# MAGIC - latitude          : latitude in decimal degrees (wgs84)
# MAGIC - longitude         : longitude in decimal degrees (wgs84)
# MAGIC - feature class     : see http://www.geonames.org/export/codes.html, char(1)
# MAGIC - feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
# MAGIC - country code      : ISO-3166 2-letter country code, 2 characters
# MAGIC - cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
# MAGIC - admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
# MAGIC - admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
# MAGIC - admin3 code       : code for third level administrative division, varchar(20)
# MAGIC - admin4 code       : code for fourth level administrative division, varchar(20)
# MAGIC - population        : bigint (8 byte int) 
# MAGIC - elevation         : in meters, integer
# MAGIC - dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
# MAGIC - timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
# MAGIC - modification date : date of last modification in yyyy-MM-dd format

# COMMAND ----------

import os
raw_openaq_data=f"{dbh.volume_directory()}/raw_openaq_data"
os.makedirs(raw_openaq_data, exist_ok=True)
indian_cities=f"{dbh.volume_directory()}/IN.txt"

indian_cities_tb=f"{dbh.table_location()}.indian_cities"
city_location_mapping_tb=f"{dbh.table_location()}.city_location_mapping"
city_location_mapping_25_tb=f"{dbh.table_location()}.city_location_25_mapping"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

geonames_schema = StructType([
    StructField("geonameid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("asciiname", StringType(), True),
    StructField("alternatenames", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("feature_class", StringType(), True),
    StructField("feature_code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("cc2", StringType(), True),
    StructField("admin1_code", StringType(), True),
    StructField("admin2_code", StringType(), True),
    StructField("admin3_code", StringType(), True),
    StructField("admin4_code", StringType(), True),
    StructField("population", IntegerType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("dem", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("modification_date", StringType(), True)
])
df = spark.read.csv(indian_cities, sep="\t", schema=geonames_schema)

# COMMAND ----------

if not dbh.table_exists(indian_cities_tb):
    df.write.mode("overwrite").saveAsTable(indian_cities_tb)

# COMMAND ----------

# MAGIC %sql
# MAGIC select asciiname, latitude, longitude,population, elevation, country_code 
# MAGIC from moyalanjintos_catalog.aura_india.indian_cities where population>100000 and feature_code in ("PPL","PPLA","PPLC");

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching AQIData from OpenAQ
# MAGIC

# COMMAND ----------

df = spark.read.csv(
    "s3://openaq-data-archive/records/csv.gz/locationid=407"
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching location Id in openaq for each city
# MAGIC - Within 25 km range
# MAGIC - Having a sensor of pm25 ( this goes deep into lung tissue)

# COMMAND ----------

import pandas as pd
import time
import random
from openaq import OpenAQ

API_KEY = "6b65bfafcaab8f13b29fde36092e190cf9c8141593fe66c1296e3a9556e09a8c"

def map_city_to_location(pdf_iter):
    client = OpenAQ(api_key=API_KEY)
    
    for pdf in pdf_iter:
        results = []
        
        for row in pdf.itertuples(index=False):
            try:
                radius = 5000
                location_id = None
                api_results =None
                
                while radius <= 25000 and location_id is None:
                    res = client.locations.list(
                        coordinates=(row.latitude, row.longitude),
                        radius=radius,
                        limit=10
                    )
                    
                    headers = res.headers
                    remaining = headers.x_ratelimit_remaining
                    reset = headers.x_ratelimit_reset
                    
                    sleep_time = max(1.0, reset / max(1, remaining))
                    time.sleep(sleep_time + random.uniform(0, 0.1))
                    
                    api_results = res.dict().get("results", [])
                    
                    valid_loc = None
                    
                    for loc in api_results:
                        sensors = loc.get("sensors", [])
                        
                        has_pm25 = any(
                            s.get("parameter", {}).get("name") == "pm25" 
                            for s in sensors
                        )
                        is_cpcb = loc.get("provider", {}).get("name", "").lower() == "cpcb"
                        
                        if has_pm25:
                            valid_loc = loc
                            break
                    
                    if valid_loc:
                        location_id = valid_loc["id"]
                        api_results = valid_loc  # overwrite with selected location
                    else:
                        radius += 10000
                
                results.append((
                    str(row.asciiname),
                    float(row.latitude),
                    float(row.longitude),
                    int(radius) if radius else None,
                    int(location_id) if location_id else None,
                    api_results["distance"] ,
                    "SUCCESS" if location_id else "NOT_FOUND",
                    api_results["name"],
                    api_results["is_mobile"],
                    api_results["is_monitor"],
                    api_results["datetime_first"]["utc"],
                    api_results["datetime_last"]["utc"]
                ))
            
            except Exception as e:
                results.append((
                    str(row.asciiname),
                    float(row.latitude),
                    float(row.longitude),
                    None,
                    None,
                    None,
                    f"Error {e}",
                    "",
                    None,
                    None,
                    None,
                    None      
                ))
        
        # ✅ correct DataFrame creation
        final_pdf = pd.DataFrame(
            results,
            columns=[
                "city", "latitude", "longitude",
                "radius", "location_id",
                "distance", "status","name", "is_mobile", "is_monitor","datetime_first", "datetime_last",
            ]
        )
        
        # ✅ enforce types (Arrow-safe)
        final_pdf = final_pdf.astype({
            "city": "string",
            "latitude": "float64",
            "longitude": "float64",
            "radius": "Int64",
            "location_id": "Int64",
            "distance": "float64",
            "status": "string",
            "name": "string",
            "is_mobile":"boolean",
            "is_monitor" :"boolean",
        })
        
        # ✅ proper timestamp handling
        final_pdf["datetime_first"] = pd.to_datetime(final_pdf["datetime_first"], utc=True, errors="coerce")
        final_pdf["datetime_last"] = pd.to_datetime(final_pdf["datetime_last"], utc=True, errors="coerce")
        
        yield final_pdf

# COMMAND ----------

cities_df = spark.table(indian_cities_tb).where(
    "population > 100000 and feature_code in ('PPL','PPLA','PPLC')"
)

# COMMAND ----------

if not dbh.table_exists(city_location_mapping_25_tb):
    mapping_df = cities_df.mapInPandas(
        map_city_to_location,
        schema="""
            city string,
            latitude double,
            longitude double,
            radius long,
            location_id long,
            distance double,
            status string,
            name string,
            is_mobile boolean,
            is_monitor boolean,
            datetime_first timestamp,
            datetime_last timestamp
        """,
    ).collect()
    mapping_df = spark.createDataFrame(mapping_df)
    mapping_df.write.mode("overwrite").saveAsTable(city_location_mapping_25_tb)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * From  moyalanjintos_catalog.aura_india.city_location_25_mapping where status='SUCCESS';

# COMMAND ----------

from pyspark.sql.functions import explode, array, lit, concat

location_df = spark.table(
    city_location_mapping_25_tb
).filter("status = 'SUCCESS'") \
 .select("location_id") \
 .distinct()

# years=[2023, 2024]


# df_expected = location_df.withColumn(
#     "year",
#     explode(array(*[lit(y) for y in years]))
# )
df_expected=location_df

# COMMAND ----------

if len(dbutils.fs.ls(raw_openaq_data)) > 0:
    df_existing = spark.read.parquet(raw_openaq_data) \
        .select( "location_id") \
        .distinct()
    df_missing = df_expected.join(
                df_existing,
                on=["location_id"],
                how="left_anti"
            )
else:
    df_missing = df_expected

# COMMAND ----------

df_missing.display()

# COMMAND ----------

df_missing = df_missing.repartition(2)

# COMMAND ----------

import pandas as pd

def fetch_and_write(pdf_iter):
    for pdf in pdf_iter:
        for _, row in pdf.iterrows():
            location_id = row["location_id"]
            year = row["year"]
            
            try:
                path = f"s3://openaq-data-archive/records/csv.gz/locationid={location_id}/year={year}"
                
                df = spark.read.option("header", True).csv(path)
                
                output_path = f"${raw_openaq_data}/{year}/{location_id}"
                
                df.write.mode("overwrite").parquet(output_path)
            
            except Exception:
                continue
        
        yield pd.DataFrame({"status": ["done"]})

# COMMAND ----------

from pyspark.sql.functions import col
df_missing = df_missing.withColumn(
    "path",
    concat(
        lit("s3://openaq-data-archive/records/csv.gz/locationid="),
        col("location_id")
    )
)

# COMMAND ----------

df_missing.display()

# COMMAND ----------

paths = df_missing.select("path").rdd.map(lambda x: x.path).collect()

# COMMAND ----------

valid_paths = []
missing_paths = []

for p in paths:
    try:
        files = dbutils.fs.ls(p)
        
        # check if actual data exists
        if len(files) > 0:
            valid_paths.append(p)
        else:
            missing_paths.append(p)
    
    except:
        missing_paths.append(p)

# COMMAND ----------

len(missing_paths)

# COMMAND ----------

df = spark.read.option("header", True)\
.option("basePath", "s3://openaq-data-archive/records/csv.gz/")\
.csv(valid_paths)

# COMMAND ----------

df.write \
  .partitionBy("year", "locationid") \
  .mode("append") \
  .parquet(raw_openaq_data)

# COMMAND ----------

df.write \
  .partitionBy("locationId") \
  .mode("append") \
  .parquet(raw_openaq_data)
