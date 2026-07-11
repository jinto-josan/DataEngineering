# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise
# MAGIC
# MAGIC | Question | Context |
# MAGIC | -------- | --------|
# MAGIC |**When was the last connection time of a Charge Point?** | A singular Charge Point sends a heartbeat message at a configured interval unless specified differently by the CSMS when it first registers itself. We can find out when it was last responsive by finding the timestamp of the most recent message from any OCPP action for that Charge Point.|
# MAGIC
# MAGIC In this exercise, we'll answer the question: **When was the last connection time of a Charge Point?**
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC Basically we will learn about data ingestion and some simple Spark's transformations here in this notebook. In the next notebooks, we will learn about a few more advanced Spark transformations. You will also see that we start using a helper API with unit and integration tests, abstracted at the moment. The details of these tests should come up in the upcoming modules.
# MAGIC
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC Let us get started with the exercise then...
# MAGIC
# MAGIC So after the Charge Point has registered itself with the CSMS, it sends OCPP messages to the CSMS (for example: StartTransaction, StopTransaction, MeterValues, etc). If there are no OCPP-messages sent during the heartbeat interval, it sends a Heartbeat message to denote its responsiveness.
# MAGIC
# MAGIC We can find out when it was last connected by finding the timestamp of the most recent message from any OCPP action for that Charge Point.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up this notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using _Shift + Enter_. 
# MAGIC
# MAGIC **NOTE:** that if your cluster shuts down, you will need to re-run the cells in this section.

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

exercise_name = "last_connection_time_of_charge_point"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Read Data
# MAGIC
# MAGIC #### Context
# MAGIC The first step in handling data in Spark (especially if the data already exists) is to **read that data into Spark** as a **DataFrame**. Data can come in various formats (CSV, JSON, Parquet, Avro, Delta Lake, etc) and Spark has several API methods to read these specific formats.
# MAGIC
# MAGIC | Format | Example |
# MAGIC | --- | --- |
# MAGIC | **CSV** | `df = spark.read.format("csv").load("/tmp/data.csv")` |
# MAGIC | **JSON** | `df = spark.read.format("json").load("/tmp/data.json")` |
# MAGIC | **Parquet** | `df = spark.format("parquet").load("/tmp/data.parquet")` |
# MAGIC
# MAGIC There are additional options that can be provided:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC spark.read.format("csv") \
# MAGIC       .option("header", True) \
# MAGIC       .option("inferSchema", True) \
# MAGIC       .option("delimiter", ",") \
# MAGIC       .load("/tmp/data.csv")
# MAGIC ```
# MAGIC
# MAGIC In this example, we are reading a CSV file, denoting that there is a header row, we expect our delimiter to be a comma, and we would like Spark to simply guess (infer) what the schema of the file is. Letting Spark infer the schema *works* but doesn't necessarily yield the most accurate reads.
# MAGIC
# MAGIC It is recommended to define a **schema** and supply that at the time of reading:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```   
# MAGIC custom_schema = StructType([
# MAGIC   StructField("animal", StringType(), True),
# MAGIC   StructField("count", IntegerType(), True),
# MAGIC ])
# MAGIC       
# MAGIC spark.read.format("csv") \
# MAGIC   .option("header", True) \
# MAGIC   .option("delimiter", ",") \
# MAGIC   .schema(custom_schema) \
# MAGIC   .load("/tmp/data.csv")
# MAGIC ```
# MAGIC
# MAGIC It's worth noting that Spark (at the time of writing) cannot read directly from a URL, only File Systems. This works great when your data is in your local filestore, HDFS, or blob storage like AWS S3, but not when you need to pull fresh data remotely.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's go!
# MAGIC In this Exercise, we'll learn how to read data into Spark and create a DataFrame from it. 

# COMMAND ----------

# MAGIC %md
# MAGIC Recall that we need to download the data to our local directory in order for Spark to access it. Let's download the file (using one of our helper functions) that we'll use as part of this exercise.

# COMMAND ----------

url = "https://raw.githubusercontent.com/data-derp/exercise-ev-databricks/main/data/1679387766.csv"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reflect
# MAGIC What kind of file are we working with? What format?

# COMMAND ----------

# MAGIC %md
# MAGIC **Your Turn**: Use the `spark.read.format("csv")` function to read in the file that we've just downloaded. Don't forget to include the delimiter, schema, and the fact that there is a header.
# MAGIC
# MAGIC Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true) 
# MAGIC  |-- message_type: integer (nullable = true) 
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  ```

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


### YOUR CODE HERE
schema=StructType([
    StructField("message_id", StringType(), True),
    StructField("message_type", StringType(), True),
    StructField("charge_point_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("write_timestamp", StringType(), True),
    StructField("body", StringType(), True)
])
def create_dataframe(filepath: str) -> DataFrame:
### Put your code here.
    return spark.read.format("csv") \
      .option("header", True) \
      .schema(schema)\
      .option("delimiter", ",") \
      .load(filepath)

df = create_dataframe(filepath)
df.show()
display(df)

# COMMAND ----------

def test_create_dataframe():
    result = create_dataframe(filepath)
    assert result is not None
    assert sorted(result.columns) == sorted(['message_id', 'message_type', 'charge_point_id', 'action', 'write_timestamp', 'body'])
    result_count = result.count()
    expected_count = 46322
    assert result_count == expected_count, f"Expected {expected_count} but got {result_count}"
    print("All tests pass! :)")
    
test_create_dataframe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Convert String to Timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC In order to find the most recent message from each Charge Point, we need to sort each of the Charge Point messages by the `write_timestamp` field. However, note that the `write_timestamp` field is a String:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC ```
# MAGIC
# MAGIC Therefore, a sort on `write_timestamp` will yield an alphabetical sorting as opposed to a proper time-based sorting.

# COMMAND ----------

# MAGIC %md
# MAGIC **Your Turn**: using the `withColumn` and `to_timestamp` methods, add a new column to the DataFrame called `converted_timestamp` which is of `timestamp` type.

# COMMAND ----------

from pyspark.sql.functions import try_to_timestamp

def convert_to_timestamp(input_df: DataFrame) -> DataFrame:
### Put your code here.
    return input_df.withColumn("converted_timestamp", try_to_timestamp("write_timestamp"))
    
df.transform(convert_to_timestamp).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_convert_to_timestamp

test_convert_to_timestamp(spark, convert_to_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC We can now sort the entire dataframe by timestamp, with the most recent messages at the top!

# COMMAND ----------

from pyspark.sql.functions import col

df.transform(convert_to_timestamp).sort(col("converted_timestamp").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Find the most recent message per Charge Point
# MAGIC In reality, we actually need the most recent message PER distinct Charge Point ID. We can use a [GroupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html?highlight=sql%20groupby) statement, [Windows](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window.html?highlight=window#pyspark.sql.functions.window), [OrderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html?highlight=orderby#pyspark.sql.DataFrame.orderBy), and [Sorting](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html?highlight=sql%20sorting) in order to achieve this. 

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's note the unique Charge Points available to us in the data:

# COMMAND ----------

from pyspark.sql.functions import col

df.select("charge_point_id").distinct().sort(col("charge_point_id").asc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 First think about how you would do this in SQL (using GroupBy, Windows, OrderBy, and sorting) and then translate it to the Spark API
# MAGIC
# MAGIC **Note** The solution to the exercise below is a bit not-trivial. You might have to use a combination of row_number and partition from pyspark sql.window and functions package
# MAGIC
# MAGIC 😉 Also, if you happen to add a row number, use `rn` as the field name.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def most_recent_message_of_charge_point(input_df: DataFrame) -> DataFrame:
### Put your code here.
    return input_df.withColumn("rn", row_number().over(Window.partitionBy("charge_point_id").orderBy(col("converted_timestamp").desc()))).\
        filter(col("rn") == 1)
    
df.transform(convert_to_timestamp).transform(most_recent_message_of_charge_point).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit test!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_most_recent_message_of_charge_point

test_most_recent_message_of_charge_point(spark, most_recent_message_of_charge_point)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Cleanup
# MAGIC Hooray! We now have the list of Charge Points with the timestamp of their most recent message. However, did you notice that there's this weird `rn` column leftover from the last transformation?

# COMMAND ----------

df.transform(convert_to_timestamp).transform(most_recent_message_of_charge_point).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's clean that up. Use the drop function to remove the `rn` column and the `write_timestamp` column, as we have converted it to `converted_timestamp`.

# COMMAND ----------

def cleanup(input_df: DataFrame) -> DataFrame:
### Put your code here.
    return input_df.drop("rn","write_timestamp")


# COMMAND ----------

# MAGIC %md
# MAGIC Let's run the unit tests!

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_cleanup

test_cleanup(spark, cleanup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## All together now!
# MAGIC Go ahead and inspect your final Data Frame!

# COMMAND ----------

final_df = df.transform(convert_to_timestamp).\
    transform(most_recent_message_of_charge_point).\
    transform(cleanup)
display(final_df)

# COMMAND ----------

from exercise_ev_databricks_unit_tests.last_connection_time_of_charge_points import test_final

test_final(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC * What does each row represent?
# MAGIC * Which column contains the relevant final communication time of the charger?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-Up
# MAGIC
# MAGIC You've been doing a great job, congratulations, you keep learning Python and Apache Spark through creating Databricks Notebooks. Until now you have learned how to read a CSV File, create the schema (Struct), add new columns and transform it, and use Windows functions. Click on the Next Module to continue working in the exercise and solving more business questions related to the domain.

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                                | Next Module                                                                                                              |
# MAGIC |-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./3.3 Practice Domain Introduction" target="_self">Practice Domain Introduction</a> | <a href="$../Module 04 - Advanced Transformations/4.0 Advanced Transformations" target="_self">Advanced Transformations</a> |

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>