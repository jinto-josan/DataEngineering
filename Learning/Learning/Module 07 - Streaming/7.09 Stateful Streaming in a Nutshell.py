# Databricks notebook source
# MAGIC %md
# MAGIC ## Streaming in a nutshell
# MAGIC
# MAGIC
# MAGIC <img src="./assets/stateful-streaming-in-a-nutshell.png" style="margin:auto"/>
# MAGIC
# MAGIC We have come a long way, haven't we? We started with learning the basics of Streaming as a concept. We then moved on to Spark Structured Streaming and appreciated its similarities with Spark SQL with the help of a few examples. Next, we differentiated between Stateless and Stateful programs and lastly, how to handle late data using windows aggregations and fault tolerance through checkpointing.
# MAGIC
# MAGIC To summarise, we may use some or all of the above topics in our applications but the typical elements of a Spark Streaming Application would look like this:
# MAGIC
# MAGIC - Streaming Source (production-grade applications typically use Apache Kafka or a similar technology)
# MAGIC - Spark Streaming Application (Stateless or Stateful, depending upon the use case)
# MAGIC - Checkpointing for Fault Tolerance
# MAGIC - Output Sink (production-grade applications will dump the data into a data store to be picked up by the end users of this data)
# MAGIC - End Users of Data (Tableau, Matplotlib, ML Pipelines, etc) pick up the near real-time data to drive insights or other products requiring near-real time data
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                                                                           | Next Topic                                                                     |
# MAGIC |--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
# MAGIC | <a href="$./7.08 Handling Late Data - Sliding Windows with Watermarks" target="_self">7.08 Handling Late Data - Sliding Windows with Watermarks</a> | <a href="$./7.10 Streaming Exercise" target="_self">7.10 Streaming Exercise |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>