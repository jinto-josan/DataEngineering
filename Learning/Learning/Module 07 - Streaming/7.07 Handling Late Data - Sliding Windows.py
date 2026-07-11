# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC **Note** This is a bonus exercise. Please do it only if you are running on schedule !
# MAGIC
# MAGIC While Tumbling Time Windows fit well with use cases that require static buckets, it is necessary to employ Sliding Time Windows for use cases that require more flexibility, like calculating an average for a 1 minute window every 15 seconds. Sliding Time windows are aptly named as they slide across a data stream, or move as time progresses. Unlike Tumbling Windows, Sliding Windows are overlapping windows and a single event can belong to more than 1 window.
# MAGIC
# MAGIC A sample use case requiring the usage of Sliding Time Windows could be: calculate the maximum price of a stock symbol in a stock market application every 30 seconds in 1 minute windows. The previous implementation of Tumbling Windows will not work here because new windows are required to be generated every 30 seconds (and they must overlap).
# MAGIC
# MAGIC Lets look at a working example, but first, the required set up

# COMMAND ----------

# Some prep work to create and clean up your working directory first

%pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
%pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "sliding_windows"

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
eventTime = "2023-04-24 12:05:31"
symbol = "AAPL"
price = "600"

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
# MAGIC Moving on to our streaming program, we will add a 3rd argument of 30 seconds to the window declaration and also the aggregate function to max(price) as MaximumPrice as shown in the code below (we are just not keeping the watermark yet. We will look at it in in the next notebook, just like Tumbling Windows)

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
# MAGIC Let us start with sending the first event from Cell 6 as  `2023-04-24 12:05:15`,`AAPL`,`500`, run the streaming program in Cell 8 and check the output in the driver logs.
# MAGIC
# MAGIC Two time windows should be displayed in the driver output. The event time of the streaming data is 12:05:15 so as expected, the window **[2023-04-24 12:05:00, 2023-04-24 12:06:00]** is displayed. Additionally, our event (streamed at 12:05:15) also is a member of the preceding and overlapping window **[2023-04-24 12:04:30, 2023-04-24 12:05:30]**. Because the window duration is 1 minute and the sliding time is 30 seconds, this event falls into two windows.
# MAGIC
# MAGIC Let us now send another data event from Cell 6 as  `2023-04-24 12:04:31`,`AAPL`,`450` and check the output in the Drive logs.
# MAGIC
# MAGIC The data was streamed at 12:04:31 with a price of 450. This is a member of the **[2023-04-24 12:04:30, 2023-04-24 12:05:30]** window. Now this window already had the MaximumPrice of 500 from the previous micro-batch. The resulting aggregated value under MaximumPrice remains at 500 because the existing value (500) is greater than the value of the event of the current microbatch (450).
# MAGIC
# MAGIC Since we have defined the sliding time as 30 seconds, the value (450) streamed at 12:04:31 also registers itself in the **[2023-04-24 12:04:00, 2023-04-24 12:05:00]** time window, which is why we also see that window in the above output.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Let us take a moment now to summarize the total number of sliding windows we have in the state store as of now
# MAGIC
# MAGIC | Window | Maximum Price | Persisted in Statestore ? | Appears in the output ? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | [2023-04-24 12:04:00, 2023-04-24 12:05:00] | 450  | True | True |
# MAGIC | [2023-04-24 12:04:30, 2023-04-24 12:05:30] | 500  | True | True |
# MAGIC | [2023-04-24 12:05:00, 2023-04-24 12:06:00] | 500  | True | False |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Let us now send another data event Cell 6 as `2023-04-24 12:05:31`,`AAPL`,`460` and check the output in the driver logs. 
# MAGIC The data was streamed at 12:05:31 with 460 as the price. This registers it under the **[2023-04-24 12:05:00, 2023-04-24 12:06:00]** window which already has the MaximumPrice of 500 from the previous micro-batch and because 500 is greater than 460, the MaximumPrice is remains at 500. This additionally validates that the window had been in the statestore from the previous micro-batch.
# MAGIC
# MAGIC Like the previous micro-batch, the event also registers itself in the **[2023-04-24 12:05:30, 2023-04-24 12:06:30]** time window, which is also displayed in the Driver logs output.

# COMMAND ----------

# MAGIC %md
# MAGIC In summary, we have 4 windows in statestore now as below
# MAGIC
# MAGIC | Window | Maximum Price | Persisted in Statestore ? | Appears in the output ? |
# MAGIC | --- | --- | --- | --- |
# MAGIC | [2023-04-24 12:04:00, 2023-04-24 12:05:00] | 450  | True | False |
# MAGIC | [2023-04-24 12:04:30, 2023-04-24 12:05:30] | 500  | True | False |
# MAGIC | [2023-04-24 12:05:00, 2023-04-24 12:06:00] | 500  | True | True |
# MAGIC | [2023-04-24 12:05:30, 2023-04-24 12:06:30] | 460  | True | True |

# COMMAND ----------

# MAGIC %md
# MAGIC Let’s send an event in the time range of the oldest window that we have in the statestore to prove that it still exists. Send a data event from netcat as` 2023-04-24 12:04:32`,`AAPL`,`470` and check the output in the Driver Logs.
# MAGIC
# MAGIC We do indeed see the oldest window appearing with the MaximumPrice of 470 and we also see a second window. The 3rd and 4th windows are not displayed, but from previous experience, we know that they are still in the statestore !
# MAGIC
# MAGIC Congratulations, you have implemented a the sliding windows now ! Next we will see the Watermarks with the sliding windows

# COMMAND ----------

# MAGIC %md
# MAGIC Lets do the clean up before the next notebook then

# COMMAND ----------

helpers.clean_working_directory()

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                            | Next Topic                                                                               |
# MAGIC |---------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
# MAGIC |<a href="$./7.06 Handling Late Data - Tumbling Windows with Watermarks" target="_self">7.06 Handling Late Data - Tumbling Windows with Watermarks</a> | <a href="$./7.08 Handling Late Data - Sliding Windows with Watermarks" target="_self">7.08 Handling Late Data - Sliding Windows with Watermarks</a>|
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

# COMMAND ----------

# MAGIC %md
# MAGIC