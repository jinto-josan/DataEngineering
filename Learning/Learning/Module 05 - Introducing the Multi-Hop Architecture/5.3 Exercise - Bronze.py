# Databricks notebook source
# MAGIC %md
# MAGIC Let us now look at a way in which we can simplify the exercises we have already done in the previous modules. We have calculated parking time and total energy spent in somewhat the hard way previously. We will now see how we can make it simple by dividing the whole noteboke in simpler and more mangeable parts. This will be achieved by making use of the multi-hop architechture we introduced in the previous notebooks of this module. The exercises in these notebook will remain the same, you will just see that solving them have become quite easier.
# MAGIC
# MAGIC # Batch Processing - Bronze Layer
# MAGIC
# MAGIC When creating long-term storage for analytical use cases, the first step is to **ingest data** from the **source**, with a shape as close as possible to the original shape. As the first step in our data processing journey, this allows us to:
# MAGIC
# MAGIC 1. create a "**checkpoint**" or "**save zone**" so that we can more easily debug issues and determine if there were issues at this step or downstream
# MAGIC 1. replay data to downstream steps in the case that there is an error or interruption (**data is idempotent**)
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC In this exercise, we will:
# MAGIC * Ingest the raw data in a single pull
# MAGIC * Convert the data to parquet format (a format good for downstream reading)
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 25 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# And uninstall a few helpers we prepared for you
%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests

# now install the databricks helpers 
# %pip install git+https://github.com/data-derp/databricks_helpers.git@sr/dbr_17.3_lts_testing
%pip install git+https://github.com/data-derp/databricks_helpers.git

# # now install and also the databricks test cases
# %pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests.git@sr/dbr_17.3_lts_testing
%pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests.git

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers
exercise_name = "batch_processing_bronze_ingest"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()
print(f"Your current working directory is: {working_directory}")

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read OCPP Data
# MAGIC We've done this a couple of times before! Run the following cells to download the data to local storage and create a DataFrame from it.

# COMMAND ----------

url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1680355141.csv.gz"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_dataframe(filepath: str) -> DataFrame:
    
    custom_schema = StructType([
        StructField("message_id", StringType(), True),
        StructField("message_type", IntegerType(), True),
        StructField("charge_point_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("write_timestamp", StringType(), True),
        StructField("body", StringType(), True),
    ])
    
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("escape", "\\") \
        .schema(custom_schema) \
        .load(filepath)
    return df
    
df = create_dataframe(filepath)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## EXERCISE: Write to Parquet
# MAGIC Now that we have our ingested data represented in a DataFrame, let's use the [`parquet writer`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.parquet.html) along with [`mode="overwrite"`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.parquet.html) to formally write our data to the specified `out_dir`.

# COMMAND ----------

def write(input_df: DataFrame):
    out_dir = f"{working_directory}/output/"
    
### Put your code here.
    mode_name = "overwrite"
    
    input_df. \
        write. \
        mode(mode_name). \
        parquet(out_dir)
    
    
write(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's inspect what we've created.

# COMMAND ----------

dbutils.fs.ls(f"{working_directory}/output/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_bronze import test_write_e2e

test_write_e2e(dbutils.fs.ls(f"{working_directory}/output"), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC A bit of clean up before we move on...

# COMMAND ----------

helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                                        | Next Topic                                                              |
# MAGIC |-------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
# MAGIC | <a href="$./5.2 Multi-Hop Architecture - Applied" target="_self">Multi-Hop Architecture - Applied</a> | <a href="$./5.4 Exercise - Silver" target="_self">Exercise - Silver</a> |

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>