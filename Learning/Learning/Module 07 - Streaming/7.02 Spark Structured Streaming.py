# Databricks notebook source
# MAGIC %md
# MAGIC ## Structured Streaming
# MAGIC
# MAGIC Spark Structured Streaming is the streaming module of the Apache Spark framework. It provides a fast, scalable, fault-tolerant stream processing engine built on top of Spark SQL module with multi-delivery semantics. In fact, Spark Streaming APIs are an extension or superset of Spark SQL API. The module, by default, provides a low latency (with end-to-end latencies as low as 100 milliseconds) micro-batching features. Additionally, from Spark 2.3 onwards, a new low-latency processing mode called Continuous Processing was introduced, which can achieve end-to-end latencies as low as 1 millisecond with at-least-once guarantees. Please note that Continuous mode is still experimental as of Spark 3.x.
# MAGIC
# MAGIC Reading and Writing in Spark Structured Streaming is different from reading and writing in Spark SQL (used in batch transformations).
# MAGIC
# MAGIC Here is an example of a CSV file read in Spark SQL:
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC
# MAGIC temp_df = spark.read \
# MAGIC         .option("header", "True") \
# MAGIC         .format("csv") \
# MAGIC         .load(file_path2)
# MAGIC ```
# MAGIC
# MAGIC And a similar example in Spark Structured Streaming where we read data from a socket source. Note: there are other [sources](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets) available in the API, such as: File Source, Kafka Source and Rating Source.
# MAGIC ```python
# MAGIC
# MAGIC lines = spark \
# MAGIC     .readStream \
# MAGIC     .format("socket") \
# MAGIC     .option("host", "localhost") \
# MAGIC     .option("port", 9999) \
# MAGIC     .load()
# MAGIC ```
# MAGIC
# MAGIC A few interesting points to note from the above code snippets :
# MAGIC
# MAGIC - The read operations in both the examples are quite similar (spark.read in case of Spark SQL and a similar spark.readStream for Spark Streaming).
# MAGIC - Both the examples create dataframes (temp_df and lines df).
# MAGIC - Both examples use the same optimised Spark SQL engine.
# MAGIC - The Spark SQL example uses the format operation to specify the format of data being read. Quite similarly, the Spark Streaming example uses the format operation to specify the source from where the data is read.
# MAGIC - Options is another useful operation which can be seen in both the example
# MAGIC - Finally the load method, which in case of Spark SQL starts loading the data from the given path, whereas in case of Spark Streaming, it starts a read thread from the given source.
# MAGIC
# MAGIC There are similarities in writing the data too. Let us see this again with an example.
# MAGIC
# MAGIC ```python
# MAGIC
# MAGIC df.write\
# MAGIC     .option("header", "true")\
# MAGIC     .format("csv")\
# MAGIC     .save(f'{OUTPUT_DIR}/sample-write')
# MAGIC ```
# MAGIC
# MAGIC A similar example in Spark Streaming for writing a stream:
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC
# MAGIC query = lines_DF \
# MAGIC     .writeStream \
# MAGIC     .outputMode("append") \
# MAGIC     .format("console") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC A few notes about the two “write” code snippets :
# MAGIC
# MAGIC - The write operations in both the examples are again quite similar (spark.write in case of Spark SQL and a similar spark.writeStream for Spark Streaming).
# MAGIC - Both the examples write their respective dataframes (df for Spark SQL and lines_DF for Spark Streaming) to an external location .
# MAGIC - Both examples use the same optimised Spark SQL engine while writing the data.
# MAGIC - The Spark SQL example uses the format operation to specify the format in which the data is written. Quite similarly, the Spark Streaming example uses the format operation to specify where the data would be written. Spark Streaming calls it a sink (console in the above example. Details on other types of sinks can be found [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks))
# MAGIC - Finally the “save” method in Spark SQL example which is a Spark Action and triggers the write to the external location. The corresponding Action in Spark Streaming is “start” which triggers the write thread to the console.
# MAGIC
# MAGIC Now in the write example for Spark Streaming code snippet above, you might have observed that an operation called “outputMode” is being used. This operation specifies what gets written to the output sink. There are few output mode options available as follows :
# MAGIC
# MAGIC - **Append Mode (Default)** - Append mode is like an “insert only” operation. This mode is most useful for stateless use cases where we want to just process the data on a per-micro batch without maintaining any state across the micro batches.
# MAGIC - **Update Mode** - This mode is like an upsert operation where the old records are updated and new records are added.
# MAGIC - **Complete Mode** - This is the mode where all the data is maintained and stored by Spark run time. This mode is most useful in stateful use cases where we want to maintain state across the micro batches.
# MAGIC
# MAGIC More details regarding output modes can be found [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes).
# MAGIC
# MAGIC **References (Bonus)**
# MAGIC
# MAGIC A complete reference structured streaming including all the concepts explained above can be found [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                            | Next Topic                                                                                          |
# MAGIC |---------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
# MAGIC | <a href="$./7.01 Intro to Streaming" target="_self">7.01 Intro to Streaming</a> | <a href="$./7.03 Stateful vs Stateless Streaming" target="_self">7.03 Stateful vs Stateless Streaming</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>