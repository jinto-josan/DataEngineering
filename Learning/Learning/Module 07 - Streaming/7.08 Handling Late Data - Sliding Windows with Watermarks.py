# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC **Note** This is a bonus exercise. Please do it only if you are running on schedule !
# MAGIC
# MAGIC You might have observed that all the data of all the 4 windows in the previous example is saved in the statestore forver. This is happening because the application has notyet set up the Watermark as we have seen in the Tumbling Windows example. 
# MAGIC
# MAGIC Lets look at a working example of it, but first, the required set up

# COMMAND ----------

# Some prep work to create and clean up your working directory first

%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "sliding_windows_with_watermarks"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC Like previous exercises, lets start with setting up our dataframe with some default for `EventTime`, `Symbol` and `Price`

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Variables that will be used to create a DataFrame
eventTime = "2023-04-24 12:04:33"
symbol = "AAPL"
price = "490"

# Schema declaration
schema = StructType([
  StructField("eventTime", StringType(), True),
  StructField("symbol", StringType(), True),
  StructField("price", StringType(), True)
])

# Let us create a batch dataframe
df = spark.createDataFrame(data = [(eventTime,symbol,price)], schema=schema)

# Display it
display(df)

# Write it to the disk (DBFS based cloud)
df.write.mode("overwrite").format("parquet").save(f"{working_directory}/batch")


# COMMAND ----------

# MAGIC %md
# MAGIC Moving on to our streaming program, lets add a watermark of 2 mins to our sliding windows program from the previous notebook at line 28 below and run

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Schema declaration
schema = StructType([
  StructField("eventTime", StringType(), True),
  StructField("symbol", StringType(), True),
  StructField("price", StringType(), True)
])

# Like our previous example, let us start with reading the data from the source, after the schema declaration and required imports
rawDataDF = (spark.readStream
      .option("header", "True")
      .format("parquet")
      .option("path", f"{working_directory}/batch")
      .schema(schema)
        .load())

# Split the data and create columns like EventTime, Symbol and Price and store the reference in stocksDF dataframe
stocksDF = rawDataDF.withColumn("eventTime", to_timestamp(col("eventTime"), "yyyy-MM-dd HH:mm:ss"))\
                    .withColumn("price", col("price").cast(DoubleType()))


# Just adding another line of code for a watermark of 2 mins on the event time
windowedWords = stocksDF\
    .withWatermark("eventTime", "2 minutes") \
    .groupBy(window("eventTime", "1 minute", "30 seconds"), stocksDF.symbol)\
    .agg(max("price").alias("maxPrice"))

# Lastly, write the data to the output console sink:
query = windowedWords \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option('truncate', 'false') \
    .start()
    

# COMMAND ----------

# MAGIC %md
# MAGIC With the code already in place, take the help of the previous notebook [7.07 Handling Late Data - Sliding Windows]($./7.07 Handling Late Data - Sliding Windows) and run all the steps of that notebook till the end in Cell 6 and Cell 8 of this notebook. At the end of it, this notebook should have statestore with below 4 records
# MAGIC
# MAGIC | Window | Maximum Price | Persisted in Statestore ? | Appears in the output ? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | [2023-04-24 12:04:00, 2023-04-24 12:05:00] | 470  | True | False |
# MAGIC | [2023-04-24 12:04:30, 2023-04-24 12:05:30] | 500  | True | False |
# MAGIC | [2023-04-24 12:05:00, 2023-04-24 12:06:00] | 500  | True | True |
# MAGIC | [2023-04-24 12:05:30, 2023-04-24 12:06:30] | 460  | True | True |

# COMMAND ----------

# MAGIC %md
# MAGIC Excellent. Note that you have introduced `watermarks` in Cell 8 of this notebook already. To see it in action, let us send a new event from Cell 6 as `2023-04-24 12:07:01`,`AAPL`,`480`. You know now be able to see two new windows added to the statestore as below
# MAGIC
# MAGIC | Window | Maximum Price | Persisted in Statestore ? | Appears in the output ? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | [2023-04-24 12:04:00, 2023-04-24 12:05:00] | 470  | True | False |
# MAGIC | [2023-04-24 12:04:30, 2023-04-24 12:05:30] | 500  | True | False |
# MAGIC | [2023-04-24 12:05:00, 2023-04-24 12:06:00] | 500  | True | False |
# MAGIC | [2023-04-24 12:05:30, 2023-04-24 12:06:30] | 460  | True | False |
# MAGIC | [2023-04-24 12:06:30, 2023-04-24 12:07:30] | 480  | True | True |
# MAGIC | [2023-04-24 12:07:00, 2023-04-24 12:08:00] | 480  | True | True |
# MAGIC
# MAGIC Now send a new event `2023-04-24 12:04:33`,`AAPL`,`490` from the terminal. This should hypothetically register itself with two windows in the statestore: **[2023-04-24 12:04:00, 2023-04-24 12:05:00]** and **[2023-04-24 12:04:30, 2023-04-24 12:05:30]**. However, you might see that it has registered itself with the second window **[2023-04-24 12:04:30, 2023-04-24 12:05:30]**. When in doubt, check the Watermark Expiry Threshold, using the below formula:
# MAGIC
# MAGIC - Max (Event Time) - Watermark = Watermark Boundary
# MAGIC
# MAGIC The event time from the data sent in previous step is 12:07:01 (represents the max event time) so the calculation is represented as :
# MAGIC
# MAGIC - 12:07:01 - 2 minutes (Watermark) = 12:05:01
# MAGIC
# MAGIC Our current Watermark Expiry Threshold is therefore 12:05:01, which is later than the end interval of our very first Window **[2023-04-24 12:04:00, 2023-04-24 12:05:00]**, which in turn explains why our data was not registered against that window. In other words, our very first window expired and no additional data can be registered here (it is ignored). The data shown above is from our 2nd window **[2023-04-24 12:04:30, 2023-04-24 12:05:30]**. This is within the Watermark Expiry Threshold; therefore, the 2nd window is still active and exists in the statestore. The other 4 windows are also in the statestore but are simply not displayed as you would have guessed 😀
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this section, we’ve learned about how to aggregate data in both Tumbling and Sliding Time Windows which is used in situations where aggregations are made on [near] real-time data. We’ve additionally learned how to handle late data with both types of Windows, which is a very common and realistic occurrence, and under what circumstances data remains in the statestore.

# COMMAND ----------

# MAGIC %md
# MAGIC Lets do the clean up !

# COMMAND ----------

helpers.clean_working_directory()

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                            | Next Topic                                                                               |
# MAGIC |---------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
# MAGIC |<a href="$./7.07 Handling Late Data - Sliding Windows" target="_self">7.07 Handling Late Data - Sliding Windows</a> | <a href="$./7.09 Stateful Streaming in a Nutshell" target="_self">7.09 Stateful Streaming in a Nutshell</a>|
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC