# Databricks notebook source
# MAGIC %md
# MAGIC ## About Watermarks
# MAGIC Watermarks are the expiry threshold that we set up in our programs after which the older windows and their data expire. This expiry threshold for watermarks is usually driven by business requirements. Since the older windows cease to exist when watermarks are set, any late data arriving for those windows is also ignored by Spark Applications. We will start with making a few changes to our Tumbling Window program from our previous notebook to enable watermarks to see them in action below but first the set up for this notebook and the source dataframe:
# MAGIC

# COMMAND ----------

# Some prep work to create and clean up your working directory first

%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "tumbling_windows_watermarks"

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
price = "100"

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
# MAGIC We will start with adding the following Watermark code [(watermarks documentation)](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking) to the Tumbling Windows Code, just before the groupBy clause for the 1 minute window as below. We are keeping the Watermark expiry threshold of 2 minutes.

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
    .withWatermark("eventTime", "2 minute") \
    .groupBy(window("eventTime", "1 minute"), stocksDF.symbol)\
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
# MAGIC
# MAGIC Now just like the previous exercise, run the code upto Cell 6 to do the set up and run the program to create a batch of data in the dataframe. 
# MAGIC
# MAGIC Run the streaming program in Cell 8 now and check the output in the driver logs (you know how to do this now, isn't it ? 😌). This output should be nothing surprising, it would just be the output of the dataframe.
# MAGIC
# MAGIC You might see an empty batch in the driver logs too but it is safe to ignore it as it is coming due to introducing Watermarks in our application code.
# MAGIC
# MAGIC Try sending a similar data with changed price in the same time window with a changed timestamp `2023-04-24 12:00:45`,`AAPL`,`600` (Well you have to make the changes in lines in lines 4 and 6 in Cell 6 above). You should be able to see the maxPrice being updated to 600 in the logs
# MAGIC
# MAGIC Let us send the data for the next time window for the same symbol `2023-04-24 12:01:10`,`AAPL`,`100`. You will see just the new record for the next time window in the logs.
# MAGIC
# MAGIC The previous time window **[2023-04-24 12:00:00, 2023-04-24 12:01:00]** is nicely preserved in the statestore, it is just not displayed above (this is a feature).
# MAGIC
# MAGIC To validate this, try sending some data to the previous window `2023-04-24 12:00:50`,`AAPL`,`700`. The previous window will now appear with the latest maxPrice. This time the next window **[2023-04-24 12:01:00, 2023-04-24 12:02:00]** will be missing but that’s alright isn’t it?
# MAGIC
# MAGIC So the late data is still being processed by our application. That is because the Event data has still not reached the Watermark boundary. The Watermark boundary is calculated using the following formula:
# MAGIC
# MAGIC - Max (Event Time) - Watermark = Watermark Boundary
# MAGIC
# MAGIC We can check if the Watermark Boundary has reached for the late data sent above. The time at which the latest event was sent is 12:01:10 (as per 2023-04-24 12:01:10,AAPL,100 record). So the calculation will be
# MAGIC
# MAGIC - 12:01:10 - 2 mins (120 seconds) = 11:59:10
# MAGIC
# MAGIC This means that all the windows ending before 11:59:10 will expire. The ending time window for our late data is 12:01:00 which is greater than the Watermark Boundary therefore the late date is processed by our application.
# MAGIC
# MAGIC Let us now send the data that will create the new 3rd window `2023-04-24 12:03:10`,`AAPL`,`100` record. We should see a new window created for this data in the logs
# MAGIC
# MAGIC Lets calculate the Watermark boundary again
# MAGIC
# MAGIC - 12:03:10 - 2 mins (120 seconds) = 12:01:10
# MAGIC
# MAGIC The Watermark boundary comes as 12:01:10 as seen above. This is later than the end time window of our very first window which is 12:01:00. This means if we send the late data again to the very first window, it should not be processed and also the data for that window should be dropped from the statestore. 
# MAGIC
# MAGIC So let us send `2023-04-24 12:00:50`,`AAPL`,`100` again from CMD 6. You should see the batch coming up empty as below. That is just Spark’s way of showing that the older window is dropped and that the watermark is working 😌
# MAGIC
# MAGIC Lets do the clean up before the next notebook then

# COMMAND ----------

helpers.clean_working_directory()

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                            | Next Topic                                                                               |
# MAGIC |---------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./7.05 Handling Late Data" target="_self">7.05 Handling Late Data</a> | <a href="$./7.07 Handling Late Data - Sliding Windows" target="_self">7.07 Handling Late Data - Sliding Windows</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>