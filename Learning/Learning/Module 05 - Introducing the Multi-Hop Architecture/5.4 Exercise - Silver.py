# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Silver Tier
# MAGIC
# MAGIC In the last exercise, we took our data wrote it to the Parquet format, ready for us to pick up in the Silver Tier. In this exercise, we'll take our first step towards curation and cleanup by:
# MAGIC
# MAGIC * Unpacking strings containing json to JSON
# MAGIC * Flattening our data (unpack nested structures and bring to top level)
# MAGIC
# MAGIC We'll do this for:
# MAGIC
# MAGIC * `StartTransaction Request`
# MAGIC * `StartTransaction Response`
# MAGIC * `StopTransaction Request`
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 55 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using `Shift + Enter`. **NOTE:** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# And uninstall a few helpers we prepared for you
%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests

# now install the databricks helpers 
%pip install git+https://github.com/data-derp/databricks_helpers.git

# # now install and also the databricks test cases
%pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests.git

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers
exercise_name = "batch_processing_silver"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()
print(f"Your current working directory is: {working_directory}")

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the Bronze layer!
# MAGIC
# MAGIC **NOTE:** normally we'd use the EXACT data and location of the data that was created in the Bronze layer but for simplicity and consistent results [of this exercise], we're going to read in a Bronze output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!.

# COMMAND ----------

url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-bronze/output/part-00000-tid-5639432049181042996-9b69459e-aeff-43e0-8e41-01d3b2c6f5d5-37-1-c000.snappy.parquet"
filepath = helpers.download_to_local_dir(url)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
df = read_parquet(filepath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Filter
# MAGIC In this exercise, filter for the `StartTransaction` action and the "Request" (`2`) message_type.

# COMMAND ----------

def start_transaction_request_filter(input_df: DataFrame):
    action = None
    message_type = None
### Put your code here.
    action = "StartTransaction"
    message_type = 2
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_filter_unit

test_start_transaction_request_filter_unit(spark, start_transaction_request_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_filter_e2e

test_start_transaction_request_filter_e2e(df.transform(start_transaction_request_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed JSON, using [`from_json`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- connector_id: integer (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- meter_start: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- reservation_id: integer (nullable = true)
# MAGIC  ```

# COMMAND ----------

from pyspark.sql.functions import from_json, col

def start_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    new_column_name: str = None
    from_column_name: str = None
### Put your code here.
    new_column_name="new_body"
    from_column_name="body"
    return input_df.withColumn(new_column_name,from_json(from_column_name, body_schema))

    
display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_unpack_json_unit

test_start_transaction_request_unpack_json_unit(spark, start_transaction_request_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_unpack_json_e2e

test_start_transaction_request_unpack_json_e2e(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Flatten
# MAGIC In this exercise, we will flatten the nested JSON within the `new_body` column and pull them out to their own columns, using [`withColumn`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [`drop`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- id_tag: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- reservation_id: integer (nullable = true)
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:** Though this shows up as an exercise in the notebook, the solutions are already provided to make it easy for you !

# COMMAND ----------

def start_transaction_request_flatten(input_df: DataFrame):
    connector_id_column_name: str = None
    meter_start_column_name: str = None
    timestamp_column_name: str = None
    columns_to_drop: List[str] = [None, None]
    connector_id_column_name: str = "connector_id"
    meter_start_column_name: str = "meter_start"
    timestamp_column_name: str = "timestamp"
    columns_to_drop: List[str] = ["new_body", "body"]

    return input_df.\
        withColumn(connector_id_column_name, input_df.new_body.connector_id).\
        withColumn("id_tag", input_df.new_body.id_tag).\
        withColumn(meter_start_column_name, input_df.new_body.meter_start).\
        withColumn(timestamp_column_name, input_df.new_body.timestamp).\
        withColumn("reservation_id", input_df.new_body.reservation_id).\
        drop(*columns_to_drop)

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_flatten_unit

    
test_start_transaction_request_flatten_unit(spark, start_transaction_request_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_flatten_e2e

test_start_transaction_request_flatten_e2e(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Cast Columns
# MAGIC Cast the `timestamp` column to [`TimestampType`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.types.TimestampType.html?highlight=timestamptype#pyspark.sql.types.TimestampType) using [`to_timestamp`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.to_timestamp.html?highlight=to_timestamp#pyspark.sql.functions.to_timestamp)
# MAGIC **HINT:** You have to import the function from "`pyspark.sql.functions`" first.
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)  #=> updated
# MAGIC  |-- reservation_id: integer (nullable = true)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:** Though this shows up as an exercise in the notebook, the solutions are already provided to make it easy for you !

# COMMAND ----------

# Do a form-import-statement here

from pyspark.sql.functions import to_timestamp


def start_transaction_request_cast(input_df: DataFrame) -> DataFrame:
    new_column_name: str = "timestamp"
    from_column_name: str = "timestamp"

    return input_df.withColumn(new_column_name, to_timestamp(col(from_column_name)))

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten).transform(start_transaction_request_cast))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_cast_unit

test_start_transaction_request_cast_unit(spark, start_transaction_request_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_request_cast_e2e

test_start_transaction_request_cast_e2e(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten).transform(start_transaction_request_cast), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Response

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Filter
# MAGIC In this exercise, filter for the `StartTransaction` action and the "Response" (`3`) message_type.

# COMMAND ----------

def start_transaction_response_filter(input_df: DataFrame):
    action: str = None
    message_type: int = None
### Put your code here.
    action: str = "StartTransaction"
    message_type: int = 3

    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_response_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_response_filter_unit

test_start_transaction_response_filter_unit(spark, start_transaction_response_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_response_filter_e2e
    
test_start_transaction_response_filter_e2e(df.transform(start_transaction_response_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a JSON string and and create a new column `new_body` containing that parsed JSON, using [`from_json`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- id_tag_info: struct (nullable = true)
# MAGIC  |    |    |-- status: string (nullable = true)
# MAGIC  |    |    |-- parent_id_tag: string (nullable = true)
# MAGIC  |    |    |-- expiry_date: string (nullable = true)
# MAGIC ```

# COMMAND ----------

def start_transaction_response_unpack_json(input_df: DataFrame):
    id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    body_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", id_tag_info_schema, True)
    ])
    new_column_name: str = None
    from_column_name: str = None
### Put your code here.
    new_column_name: str = "new_body"
    from_column_name: str = "body"

    return input_df.withColumn(new_column_name,from_json(col(from_column_name), body_schema))
    
display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_response_unpack_json_unit

test_start_transaction_response_unpack_json_unit(spark, start_transaction_response_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_response_unpack_json_e2e
    
test_start_transaction_response_unpack_json_e2e(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Flatten
# MAGIC In this exercise, we will flatten the nested JSON within the `new_body` column and pull them out to their own columns, using [`withColumn`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [`drop`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- id_tag_info_status: string (nullable = true)
# MAGIC  |-- id_tag_info_parent_id_tag: string (nullable = true)
# MAGIC  |-- id_tag_info_expiry_date: string (nullable = true)
# MAGIC ```

# COMMAND ----------

def start_transaction_response_flatten(input_df: DataFrame):
    transaction_id_column_name: str = None
    drop_column_names: List[str] = [None, None]
### Put your code here.
    transaction_id_column_name: str= "transaction_id"
    drop_column_names: List[str] = ["new_body", "body"]

    return input_df.\
        withColumn(transaction_id_column_name, input_df.new_body.transaction_id).\
        withColumn("id_tag_info_status", input_df.new_body.id_tag_info.status).\
        withColumn("id_tag_info_parent_id_tag", input_df.new_body.id_tag_info.parent_id_tag).\
        withColumn("id_tag_info_expiry_date", input_df.new_body.id_tag_info.expiry_date).\
        drop(*drop_column_names)

display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_response_flatten_unit

test_start_transaction_response_flatten_unit(spark, start_transaction_response_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_start_transaction_response_flatten_e2e
    
test_start_transaction_response_flatten_e2e(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StopTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Filter
# MAGIC In this exercise, filter for the `StopTransaction` action and the "Request" (`2`) message_type.

# COMMAND ----------

def stop_transaction_request_filter(input_df: DataFrame):
    action: str = None
    message_type: int = None
### Put your code here.
    action:str = "StopTransaction"
    message_type:int = 2

    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(stop_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_filter_unit
    
test_stop_transaction_request_filter_unit(spark, stop_transaction_request_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_filter_e2e

test_stop_transaction_request_filter_e2e(df.transform(stop_transaction_request_filter), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a JSON string and and create a new column `new_body` containing that parsed JSON, using [`from_json`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- meter_stop: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- reason: string (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- transaction_data: array (nullable = true)
# MAGIC  |    |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import ArrayType
    
def stop_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", ArrayType(StringType()), True)
    ])
    new_column_name: str = None
    from_column_name: str = None
### Put your code here.
    new_column_name:str = "new_body"
    from_column_name:str = "body"

    return input_df.withColumn(new_column_name,from_json(col(from_column_name), body_schema))


display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_unpack_json_unit
    
test_stop_transaction_request_unpack_json_unit(spark, stop_transaction_request_unpack_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_unpack_json_e2e
    
test_stop_transaction_request_unpack_json_e2e(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Flatten
# MAGIC In this exercise, we will flatten the nested JSON within the `new_body` column and pull them out to their own columns, using [`withColumn`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [`drop`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

def stop_transaction_request_flatten(input_df: DataFrame):
    meter_stop_column_name: str = None
    timestamp_column_name: str = None
    transaction_id_column_name: str = None
    drop_column_names: List[str] = [None, None]
### Put your code here.
    meter_stop_column_name: str = "meter_stop"
    timestamp_column_name: str = "timestamp"
    transaction_id_column_name: str = "transaction_id"
    drop_column_names: List[str] = ["new_body", "body"]

    return input_df.\
        withColumn(meter_stop_column_name, input_df.new_body.meter_stop).\
        withColumn(timestamp_column_name, input_df.new_body.timestamp).\
        withColumn(transaction_id_column_name, input_df.new_body.transaction_id).\
        withColumn("reason", input_df.new_body.reason).\
        withColumn("id_tag", input_df.new_body.id_tag).\
        withColumn("transaction_data", input_df.new_body.transaction_data).\
        drop(*drop_column_names)

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_flatten_unit
    
test_stop_transaction_request_flatten_unit(spark, stop_transaction_request_flatten)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_flatten_e2e

test_stop_transaction_request_flatten_e2e(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten), spark, display)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Cast Columns
# MAGIC Cast the `timestamp` column to [`TimestampType`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.types.TimestampType.html?highlight=timestamptype#pyspark.sql.types.TimestampType) using [`to_timestamp`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.to_timestamp.html?highlight=to_timestamp#pyspark.sql.functions.to_timestamp).
# MAGIC
# MAGIC Target Schema:
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

def stop_transaction_request_cast(input_df: DataFrame) -> DataFrame:
    new_column_name: str = None
    from_column_name: str = None
### Put your code here.
    new_column_name: str = "timestamp"
    from_column_name: str = "timestamp"

    return input_df.withColumn(new_column_name, to_timestamp(col(from_column_name)))

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten).transform(stop_transaction_request_cast))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unit Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_cast_unit

test_stop_transaction_request_cast_unit(spark, stop_transaction_request_cast)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_stop_transaction_request_cast_e2e

test_stop_transaction_request_cast_e2e(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten).transform(stop_transaction_request_cast), spark, display)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Request to Parquet
# MAGIC In this exercise, write the StartTransaction Request data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_start_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionRequest"
    mode_name: str = None
### Put your code here.
    mode_name: str = "overwrite"

    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)
    

write_start_transaction_request(df.\
    transform(start_transaction_request_filter).\
    transform(start_transaction_request_unpack_json).\
    transform(start_transaction_request_flatten).\
    transform(start_transaction_request_cast))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_write_start_transaction_request

test_write_start_transaction_request(spark, dbutils, out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Response to Parquet
# MAGIC In this exercise, write the StartTransaction Response data to `f"{out_dir}/StartTransactionResponse"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_start_transaction_response(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionResponse"
    mode_name: str = None
### Put your code here.
    mode_name: str = "overwrite"
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)


write_start_transaction_response(df.\
    transform(start_transaction_response_filter).\
    transform(start_transaction_response_unpack_json).\
    transform(start_transaction_response_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionResponse")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_write_start_transaction_response
    
test_write_start_transaction_response(spark, dbutils, out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StopTransaction Request to Parquet
# MAGIC In this exercise, write the StopTransaction Request data to `f"{out_dir}/StopTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_stop_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StopTransactionRequest"
### Put your code here.
    mode_name: str = "overwrite"

    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)

write_stop_transaction_request(df.\
    transform(stop_transaction_request_filter).\
    transform(stop_transaction_request_unpack_json).\
    transform(stop_transaction_request_flatten).\
    transform(stop_transaction_request_cast))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StopTransactionRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2E Test

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_silver import test_write_stop_transaction_request
    
test_write_stop_transaction_request(spark, dbutils, out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congrats for finishing the **Batch Processing Silver Tier** exercise! We now have unpacked and flattened data for:
# MAGIC * `StartTransaction Request`
# MAGIC * `StartTransaction Response`
# MAGIC * `StopTransaction Request`
# MAGIC * `MeterValues Request`
# MAGIC
# MAGIC Hypothetically, we could have also done the same for the remaining actions (e.g. Heartbeat Request/Response, BootNotification Request/Response), but to save some time, we've only processed the actions that are relevant to the Gold layers that we'll build next (thin-slices, ftw!). You might have noticed that some of the processing steps were a bit repetitive and especially towards the end, could definitely be D.R.Y.'ed up (and would be in production code), but for the purposes of the exercise, we've gone the long route.

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                          | Next Topic                                                          |
# MAGIC |-------------------------------------------------------------------------|---------------------------------------------------------------------|
# MAGIC | <a href="$./5.2 Exercise - Bronze" target="_self">Exercise - Bronze</a> | <a href="$./5.5 Exercise - Gold" target="_self">Exercise - Gold</a> |

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>