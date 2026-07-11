# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC We have now seen that Spark Structured applications can store and aggregate data across micro-batches (A micro batch is one single run of our spark streaming program by the spark streaming background thead). Spark runtime stores this data in what Spark calls a `statestore`. This statestore uses the executor memory of the worker machines. Now a couple of questions have probably crossed your mind:
# MAGIC
# MAGIC - Does Spark statestore store the data forever? Yes, at least until the application crashes or it is brought down for routine housekeeping. Spark Streaming applications are designed to handle these kinds of interruptions in this fashion.
# MAGIC - Should we keep the data in the statestore of the Spark Streaming Application forever? Well, not really, for 2 reasons below:
# MAGIC     - As mentioned above, the statestore uses the executor memory which is finite and will sooner or later get exhausted. This usually results in either the Spark job being stuck for a very long time or ultimately crashing.
# MAGIC     - The use case might not require you to store the data forever. For example, you may have an application where you would want to see the running totals of the invoices of the last 1 hour. Every hour, you would want to move the data to an external storage and clean the statestore.
# MAGIC
# MAGIC Above are just a few examples where you might not want to maintain the data in your application forever. This is where the concept of windows and late data comes into picture.
# MAGIC
# MAGIC ## Time Windows
# MAGIC
# MAGIC Windows are an important temporal concept for managing data and its state in Spark Streaming applications. They are the time intervals on which we can segregate and aggregate the incoming streaming data from the source. The time for which such windows are created is usually the Event Time i.e the timestamp for which the data was generated at the source. Please note that the aforementioned Event Time is different from the Trigger time, which is the start time of the micro-batch by the Spark runtime. Also note that Time Windows are used only for Stateful Spark Streaming applications because, afterall, we are trying to efficiently manage the state so that our applications do not store the data forever.
# MAGIC
# MAGIC There are two types of Time Windows in Spark Streaming:
# MAGIC
# MAGIC - Tumbling Time windows
# MAGIC - Sliding Time windows
# MAGIC
# MAGIC ### Tumbling Time Windows
# MAGIC
# MAGIC Tumbling Windows are fixed, non-overlapping and contiguous (or back-to-back) time windows that we can create based on the Event time. For Eg. A 15 minute tumbling window can be created for data generated between 12:00 PM to 12:15 PM, 12:15 PM to 12:30 PM and so on. Let us understand the concept of tumbling window in more detail with the help of an example below:
# MAGIC
# MAGIC Let’s say we are developing an application which is processing the data generated from a stock market application. The stock market application is sending data in the following on periodic basis to our application:
# MAGIC
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | EventTime | String |
# MAGIC | Symbol | String |
# MAGIC | Price | String |
# MAGIC
# MAGIC
# MAGIC While we are at it, let us start building an application which help us do this. This will be the development of a simple dataframe like previous exercises as seen below. 

# COMMAND ----------

# Some prep work to create and clean up your working directory first

%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "handling_late_data"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC We will start with some starting values for `EventTime`, `Symbol` and `Price`

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Variables that will be used to create a DataFrame
eventTime = "2023-04-24 12:00:50"
symbol = "AAPL"
price = "700"

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
# MAGIC With the basic data established, let us now develop our streaming application. This application will receive the above data and create a 1 minute tumbling time window. Also, to demonstrate the windowing aggregates, our application will sum up all the prices for the stock symbol generated within each time window of 1 minute.
# MAGIC
# MAGIC Let us see the code below

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


# Create a Time Window of 1 minute. Note that Time Windows are created by using the groupBy transformation followed by some kind of aggregation as seen below. This essentially means that we are creating aggregating windows of 1 minute each which will later on help to decide which events to keep or discard.
windowedWords = stocksDF\
    .groupBy(window("eventTime", "1 minute"), stocksDF.symbol)\
    .agg(max("price").alias("maxPrice"))

# # Lastly, write the data to the output console sink:
query = windowedWords \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option('truncate', 'false') \
    .start()
    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC So while the above streaming program in Cell 8 is running, go to the `Compute`link on the sidebar and open it in a new tab. Like the previous exercise, go to click on your `Computer Instance` go to `Driver Logs` and `Standard Output`. You should be able to see the output of the first dataframe created in Cell 5 as `Batch:0` in the logs.
# MAGIC
# MAGIC Now change the price variable on Line 6, Cell 6 to `600` and execute that cell one more time. Again go to the `Standard Output` and in a bit of time, you should be able to see the second batch (as `Batch:0`) in the logs.
# MAGIC
# MAGIC Why is this happening ?
# MAGIC
# MAGIC Well, look at the code at line 27, 28 and 29 in Cell 8. We are calculating the `maxPrice` out of all the windows available in state store and dishing out the one with the maximum price !
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC With that done now, lets try a few more interesting things with our code. let us go back to the Cell 6 again and try sending the data for the next time window and see what happens. More specifically change the `eventTime`variable to `2023-04-24 12:01:10` and `price` 100 and let the `Symbol` variable be `APPL` only.
# MAGIC
# MAGIC Go to the logs again and see what happens there !
# MAGIC
# MAGIC You should see that output now with two records for two difference windows.
# MAGIC
# MAGIC Congratulations. You have implemented and learned about the `Tumbling Windows` concept

# COMMAND ----------

# MAGIC %md
# MAGIC ### Another interesting observation
# MAGIC There would be one more thing to note before we move to the next notebook. You might have noticed that the above program is maintaining the data in the `Statestore` forever, even though the Spark Application managed to segregate it into Tumbling Time Windows of 1 minute each. 
# MAGIC
# MAGIC To further validate this, try sending data to an older window (evenTime = 2023-04-24 12:00:50,symbol = AAPL, price = 700) from Cell 6 again, assuming that it has arrived a bit late to your application. You will see the older window being updated with the latest maximum price of 700 in the logs.
# MAGIC
# MAGIC What happened here ? The application retains all the data because we have defined the output mode as `Complete`. As we have understood out modes, Complete output mode is designed to retain the data in the `statestore` forever. If we want our Spark Application to clean up the data for older windows and also stop accepting any late data in those windows (often a business decision/requirement), we will have to add what Spark calls as `Watermark` to the application code.
# MAGIC
# MAGIC let that be the topic for the next section / notebook then !

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                                      | Next Topic                                                                |
# MAGIC |-----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
# MAGIC | <a href="$./7.04 Checkpointing" target="_self">7.04 Checkpointing</a> |  <a href="$./7.06 Handling Late Data - Tumbling Windows with Watermarks" target="_self">7.06 Handling Late Data - Tumbling Windows with Watermarks</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC